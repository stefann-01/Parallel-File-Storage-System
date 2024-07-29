import os
import threading
import yaml
import hashlib
import zlib
import multiprocessing
from multiprocessing.pool import ThreadPool


def load_config(config_path):
    with open(config_path, 'r') as stream:
        return yaml.safe_load(stream)


current_memory_usage = 0
memory_usage_lock = threading.Lock()
file_registry = {}
file_registry_lock = threading.Lock()
part_registry = {}
part_registry_lock = threading.Lock()
io_pool = None
memory_condition = threading.Condition()

config = load_config('config.yaml')
STORAGE_PATH = config['storage']['directory']
if not os.path.isabs(STORAGE_PATH):
    STORAGE_PATH = os.path.abspath(STORAGE_PATH)
CHUNK_SIZE = config['storage']['chunk_size']
NUMBER_OF_IO_PROCESSES = config['process_pool']['number_of_io_processes']
NUMBER_OF_COMMAND_THREADS = config['thread_pool']['number_of_command_threads']
MAX_MEMORY_USAGE = config['memory']['max_usage']


class File:
    _file_id_counter = 0
    _file_id_lock = threading.Lock()

    def __init__(self, file_name=None, number_of_parts=0, status='processing'):
        with File._file_id_lock:
            self.file_id = File._file_id_counter
            File._file_id_counter += 1
        self.file_name = file_name
        self.number_of_parts = number_of_parts
        self.status = status


class FilePart:
    _file_part_id_counter = 0
    _file_part_id_lock = threading.Lock()

    def __init__(self, file_id, sequence_number, md5_hash=None, status='processing'):
        with FilePart._file_part_id_lock:
            self.part_id = FilePart._file_part_id_counter
            FilePart._file_part_id_counter += 1
        self.file_id = file_id
        self.sequence_number = sequence_number
        self.md5_hash = md5_hash
        self.status = status


def check_and_update_memory_usage(size_required):
    global current_memory_usage
    with memory_condition:
        if current_memory_usage + size_required > MAX_MEMORY_USAGE:
            return False
        current_memory_usage += size_required
    return True


def reduce_memory_usage(size_freed):
    global current_memory_usage
    with memory_condition:
        current_memory_usage = max(0, current_memory_usage - size_freed)
        memory_condition.notify_all()


def compress_and_store_part(chunk_data, part_id, storage_path):
    global file_registry
    global part_registry
    try:
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        md5_hash = hashlib.md5(chunk_data).hexdigest()
        compressed_data = zlib.compress(chunk_data)
        with open(storage_path, 'wb') as file:
            file.write(compressed_data)
        return part_id, md5_hash
    except Exception as e:
        print(f"Error in worker process: {e}")
        raise


def decompress_and_verify_part(storage_path, expected_md5):
    global file_registry
    global part_registry
    try:
        with open(storage_path, 'rb') as file:
            compressed_data = file.read()
        decompressed_data = zlib.decompress(compressed_data)
        calculated_md5_hash = hashlib.md5(decompressed_data).hexdigest()

        if calculated_md5_hash != expected_md5:
            return None, f"MD5 hash mismatch for part {storage_path}."

        return decompressed_data, None
    except Exception as e:
        return None, f"Error processing part {storage_path}: {e}"


def handle_put(path):
    global file_registry
    global part_registry
    global io_pool
    if not os.path.exists(path):
        print(f"File {path} not found.")
        return

    if not os.path.isabs(path):
        path = os.path.abspath(path)
    new_file = File(file_name=os.path.basename(path))
    with file_registry_lock:
        file_registry[new_file.file_id] = new_file
    part_sequence_number = 0
    file_end = False
    try:
        with open(path, 'rb') as file:
            file_size = os.path.getsize(path)
            while not file_end:
                batch = []
                memory_needed = 0
                for _ in range(NUMBER_OF_IO_PROCESSES):
                    chunk_data = file.read(CHUNK_SIZE)
                    if not chunk_data:
                        file_end = True
                        break
                    memory_needed += CHUNK_SIZE
                    storage_path = os.path.join(STORAGE_PATH, 'file_parts',
                                                f'part_{new_file.file_id}_{part_sequence_number}.dat')
                    file_part = FilePart(file_id=new_file.file_id, sequence_number=part_sequence_number)
                    part_registry[file_part.part_id] = file_part
                    batch.append((chunk_data, file_part.part_id, storage_path))
                    part_sequence_number += 1

                if not check_and_update_memory_usage(memory_needed):
                    with memory_condition:
                        print("Memory limit reached, waiting for resources to free up...")
                        memory_condition.wait()
                    continue
                results = io_pool.starmap(compress_and_store_part, batch)

                for part_id, md5_hash in results:
                    part_registry[part_id].md5_hash = md5_hash
                    part_registry[part_id].status = "ready"

                reduce_memory_usage(memory_needed)

        with file_registry_lock:
            file_registry[new_file.file_id].status = "ready"
            file_registry[new_file.file_id].number_of_parts = part_sequence_number + 1
            print("Your file ID is: ", new_file.file_id)

    except FileNotFoundError:
        print(f"The file at {path} does not exist.")
    except IOError as e:
        print(f"An error occurred while accessing the file: {e}")


def handle_get(file_id):
    file_id = int(file_id)
    global file_registry
    global part_registry
    global io_pool

    with file_registry_lock:
        if file_id not in file_registry:
            print(f"File with ID {file_id} does not exist.")
            return

        file_info = file_registry[file_id]

        if not file_info or file_info.status != 'ready':
            print(f"File with ID {file_id} is not available.")
            return

    with part_registry_lock:
        file_parts = [part for part in part_registry.values() if part.file_id == file_id]
        file_parts.sort(key=lambda part: part.sequence_number)

    destination_path = os.path.join(STORAGE_PATH, 'retrieved_files', file_info.file_name)
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    try:
        with open(destination_path, 'wb') as destination_file:
            for i in range(0, len(file_parts), NUMBER_OF_IO_PROCESSES):
                batch = file_parts[i:i + NUMBER_OF_IO_PROCESSES]
                tasks = []
                memory_allocated = 0
                for part in batch:
                    if not check_and_update_memory_usage(CHUNK_SIZE):
                        with memory_condition:
                            print("Memory limit reached, waiting for resources to free up...")
                            memory_condition.wait()
                        continue
                    memory_allocated += CHUNK_SIZE
                    tasks.append((
                                 os.path.join(STORAGE_PATH, 'file_parts', f'part_{file_id}_{part.sequence_number}.dat'),
                                 part.md5_hash))

                results = io_pool.starmap(decompress_and_verify_part, tasks)
                reduce_memory_usage(memory_allocated)

                for data, error in results:
                    if error:
                        print("Error: File corrupted.")
                        destination_file.close()
                        os.remove(destination_path)
                        return
                    destination_file.write(data)

        print(f"File has been successfully retrieved and stored at {destination_path}.")

    except Exception as e:
        print(f"An error occurred while assembling the file: {e}")


def delete_file_part(storage_path, part_id):
    part_id = int(part_id)
    try:
        if os.path.exists(storage_path):
            os.remove(storage_path)
        return part_id, None
    except Exception as e:
        return part_id, e


def handle_delete(file_id):
    file_id = int(file_id)
    global file_registry, part_registry, io_pool

    file_id = int(file_id)

    with file_registry_lock:
        if file_id in file_registry:
            file_registry[file_id].status = 'not_ready'
        else:
            print(f"No file found with ID {file_id}.")
            return

    with part_registry_lock:
        file_parts = [part for part in part_registry.values() if part.file_id == file_id]
        for part in file_parts:
            part.status = 'not_ready'

    tasks = [(os.path.join(STORAGE_PATH, 'file_parts', f'part_{file_id}_{part.sequence_number}.dat'), part.part_id) for
             part in file_parts]
    results = io_pool.starmap(delete_file_part, tasks)

    for part_id, error in results:
        if error:
            print(f"Error deleting part {part_id}: {error}")
        else:
            with part_registry_lock:
                del part_registry[part_id]

    if all(result[1] is None for result in results):
        with file_registry_lock:
            del file_registry[file_id]
        print(f"File with ID {file_id} has been deleted.")
    else:
        print(f"File with ID {file_id} could not be fully deleted.")


def handle_list():
    with file_registry_lock:
        if not file_registry:
            print("No files stored.")
        else:
            print("Stored files:")
            for file_id, file_info in file_registry.items():
                print(f"ID: {file_id}, Name: {file_info.file_name}, Status: {file_info.status}")


def handle_command(cmd):
    words = cmd.split()
    if words[0] == 'list':
        handle_list()
        return
    elif len(words) <= 1:
        print("Error: bad arguments, missing file id or path.")
    else:
        if words[0] == 'put':
            handle_put(words[1])
        elif words[0] == 'get':
            handle_get(words[1])
        elif words[0] == 'delete':
            handle_delete(words[1])
        else:
            print("Error: unknown command.")


def main():
    global file_registry
    global part_registry
    with ThreadPool(NUMBER_OF_COMMAND_THREADS) as command_pool:
        while True:
            try:
                cmd = input()
                words = cmd.split()
                if len(words) > 2 or words[0] not in ('put', 'get', 'delete', 'list', 'exit'):
                    print("Error: unknown command")
                    continue
                if cmd == 'exit':
                    command_pool.close()
                    command_pool.join()
                    io_pool.close()
                    io_pool.join()
                    break
                else:
                    command_pool.apply_async(handle_command, (cmd,))
            except Exception as e:
                print(f"An error occurred: {e}")


if __name__ == '__main__':
    io_pool = multiprocessing.Pool(NUMBER_OF_IO_PROCESSES)
    main()
