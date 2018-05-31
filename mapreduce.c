#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define SEED_LENGTH 65

const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

long TASK_SIZE = 256;
char INPUT_PATH[128] = "res/wikipedia_test_small.txt";
char OUTPUT_PATH[128] = "output/results.csv";

int calculateDestRank(char *word, int length, int num_ranks);
int processFlags(int argc, char *argv[]);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank;
    int num_ranks;
    int iterations;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    if(rank == 0){
        // Process flags
        processFlags(argc, argv);

        printf("Task size: %ld \n", TASK_SIZE);
        printf("Input path: %s\n", INPUT_PATH);
        printf("Output path: %s\n", OUTPUT_PATH);

        /**
            MASTER:
            -Open file, split and assign (num_ranks - 1) blocks of 64 MB to slaves
            -Do slave work during reduce phase
            -Retrieve aggregated data from slave processes and write to file
        */

        MPI_File file;
        MPI_File_open(MPI_COMM_SELF, INPUT_PATH, 
            MPI_MODE_RDONLY, MPI_INFO_NULL, &file);

        if(file == MPI_FILE_NULL){
            printf("File does not exist.\n");
            MPI_Finalize();
            exit(0);
        }

        //Checking file_size
        MPI_Offset file_size;
        MPI_File_get_size(file, &file_size);

        // Broadcast #iterations send/receives required to process the input data
        iterations = file_size / (num_ranks * TASK_SIZE);
        MPI_Bcast(&iterations, 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Start sending data
        int offset = 0;
        for (int i; i < iterations; i++){
            for(int thread = 1; thread < num_ranks; thread++){
                //READ DATA
                char *buf = malloc(TASK_SIZE * sizeof(char));
                MPI_File_seek(file, offset, MPI_SEEK_SET);
                MPI_File_read(file, buf, TASK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);

                //SEND DATA
                MPI_Request request;
                MPI_Isend(&buf, TASK_SIZE, MPI_CHAR, thread, 0,
                  MPI_COMM_WORLD, &request);
                
                //WAIT
                MPI_Wait(&request, MPI_STATUS_IGNORE);
                //printf("Sent to process %d: \n%s\n", i, buf);
                
                offset += TASK_SIZE;
                free(buf);
            }            
        }
    }

    if(rank > 0){
        
        MPI_Bcast(&iterations, 1, MPI_INT, 0, MPI_COMM_WORLD);

        //printf("Rank %d says %d iterations\n", rank, iterations);
        
        for(int i = 0; i < iterations; i++){
            char *buf = malloc(TASK_SIZE * sizeof(char));

            MPI_Recv(&buf, TASK_SIZE, MPI_CHAR, 0, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("hello");
            //printf("%s\n", buf);
        }
    }
    /**
    SLAVE:
        -Retrieve 64 MB chunk from master
        -Until chunk is consumed, perform Map() function, storing results locally
            -The master process provides slaves with way to detect end of file
        -Redistribute all <key, value> pairs across all processes. This is done
         by calling calculateDestRank() to find out rank of receiving process.
        -After receiving redistributed <key, value> pairs, the process aggregates
         the result by calling reduce() repeatedly.
    */

    printf("rank %d says hello\n", rank);

    MPI_Finalize();
    return 0;
}

int calculateDestRank(char *word, int length, int num_ranks)
{
    uint64_t hash = 0;

    for (uint64_t i = 0; i < length; i++)
    {
        uint64_t num_char = (uint64_t)word[i];
        uint64_t seed     = (uint64_t)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return (int)(hash % (uint64_t)num_ranks);
}

/** Set parameters using flags
-task size  -n size
-input file -i path
-output file -o path
-other parameters
*/
int processFlags(int argc, char *argv[]) {
    for(size_t i = 1; i < argc; i++) {
        if(strncmp(argv[i], "-n", 2) == 0) {
            i++;
            char *eptr;
            TASK_SIZE = strtol(argv[i], &eptr, 10);
        }
        else if(strncmp(argv[i], "-i", 3) == 0) {
            i++;
            strcpy(INPUT_PATH, argv[i]);
        }
        else if(strncmp(argv[i], "-o", 2) == 0) {
            i++;
            strcpy(OUTPUT_PATH, argv[i]);
        }
    }
    return 1;
}
