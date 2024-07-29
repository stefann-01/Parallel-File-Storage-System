# # Parallel File Storage System

Implementing a parallel file storage system that splits, compresses, and stores file parts. The system supports commands for adding, retrieving, deleting files, and listing stored files.

## Problem Descriptions

### Components of the System

#### File Registry
- **Function**: Maintains a list of all stored files, their unique identifiers, and status (ready or not).

#### File Part Registry
- **Function**: Keeps track of all file parts, including their unique identifiers, file associations, sequence numbers, and MD5 hashes.

### Command Handler Thread
- **Function**: Main thread for accepting commands from the command line and spawning a new thread to handle each command.

### Commands

#### put
- **Function**: 
  - Assign a unique ID to the file.
  - Add the file to the file registry.
  - Read the file in chunks, assign IDs to parts, and add them to the part registry.
  - Process file parts via I/O processes to compute MD5 hashes, compress, and store them.

#### get
- **Function**: 
  - Retrieve file metadata and parts list.
  - Reassemble and verify file parts.
  - Handle errors in part retrieval gracefully.

#### delete
- **Function**: 
  - Mark file and parts as not ready.
  - Delete parts via I/O processes and update registries.

#### list
- **Function**: Display the list of stored files and their IDs.

#### exit
- **Function**: Shut down the system and active threads/processes.

## Running the Project
1. Ensure you have the required dependencies installed.
2. Configure the system using the `config.yaml` file.
3. Run `run.py` to start the system and interact with it via command line.

## Dependencies
- Python 3.x
- Required libraries: `os`, `threading`, `yaml`, `hashlib`, `zlib`, `multiprocessing`

## Configuration
- **Path**: Specify the directory for storing file parts.
- **Processes**: Set the number of I/O processes.
- **Memory**: Define the maximum memory usage.

## Notes
- The system handles files larger than the available memory.
- It processes commands in parallel, ensuring efficient memory usage.
