#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SEED_LENGTH 65

const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

long TASK_SIZE = 64000000;
char INPUT_PATH[128] = "res/wikipedia_test_small.txt";
char OUTPUT_PATH[128] = "output/results.csv";

int calculateDestRank(char *word, int length, int num_ranks);
int processFlags(int argc, char *argv[]);

int main(int argc, char *argv[])
{
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
-task size  -t n
-input file -p path
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
