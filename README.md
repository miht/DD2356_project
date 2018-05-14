# DD2356_project


The goal of the master process is to perform the following actions:

Map phase:
    1. Read the next (num_procs-1)*64MB blocks from the input dataset. These are
    the inputs for each Map task, that will be assigned to the particular slave
    processes accordingly.
    2. Send each 64MB block as the next task for each slave process. For
    instance, if we have 16 processes in total, the master will read 15 blocks
    of 64MB and provide one per individual process.
    3. Repeat from Step 1 until the whole input dataset is completely read.
Reduce phase:
    1. The master will act as a any other slave process. See the description for
    slaves.
Combine phase:
    1. Retrieve all the aggregated key-value pairs from each slave process. The
    idea is for the master process to grab all the intermediate key-value pairs
    to produce the final result. For simplicity, we are not going to order the
    final output.
    2. Store the result into a CSV file. The master will store each <key,value>
    using a comma-separated entry. Two fields are expected per entry in the CSV
    file: the key and the value.
________________________________________________________________________________

The goal of the slave processes is to perform the following actions:

Map phase:
    1. Retrieve the next 64MB “task” to compute. The master process is going to
    provide each slave with a 64MB task to compute, as previously described.
    2. Until the input data of the 64MB task is consumed, repeat:
        2.1 Call Map() with the current offset inside the 64MB block.
        2.2 Store locally the <key,value> output given by Map(), if any.
        2.3 Advance the offset for the next call to Map(). This fact implies
        that Map() should return a <key,value> tuple and also how much data was
        consumed from the input 64MB block.
    3. Repeat from Step 1 until the whole input dataset is completely read. You
    will need a mechanism from the master process to notify the end of the file.
Reduce phase:
    1. Exchange the intermediate <key,value> pairs across all the processes.
    Each process will receive groups of key-value tuples assigned to it, as
    described later in the document.
    2. Aggregate locally all the <key,value> tuples, calling Reduce() repeatedly
    until the values have been stored.
Combine phase:
    1. Send the aggregated key-value pairs to the master process. This will
    generate the result of the MapReduce execution.
