#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>
#include <vector>
#include <string>
#include <map>
#include <cstddef>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <iostream>
#include <sys/time.h>

#define SEED_LENGTH 65

const char word_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *word_seed_num = (unsigned short*)word_seed;

const int TAG_BUFFER = 0;

long TASK_SIZE = 256;
char INPUT_PATH[128] = "res/wikipedia_test_small.txt";
char OUTPUT_PATH[128] = "res/results.csv";

const int WORD_LENGTH = 32;

typedef struct KeyValue {
  char word[WORD_LENGTH];
  uint64_t count;
} keyvalue;

double mysecond();
int calculateDestRank(char *word, int length, int num_ranks);
int processFlags(int argc, char *argv[]);
int Map(char* buf, int len, KeyValue *pair, int *offset, int *word_len);
int Reduce(std::map<std::string, uint64_t> &reducedKeyValues, KeyValue *s_aggregated_keyvalues);
int Create_KeyValue_Datatype(MPI_Datatype *MPI_KeyValue);
int Calculate_SendRecv_Count_And_Displs(std::vector<std::map<std::string, uint64_t> > &bucket,
int *s_redistr_sizes, int *r_redistr_sizes, int *s_redistr_displs, int *r_redistr_displs, uint64_t &num_keyvalues_tot, const int num_ranks);
