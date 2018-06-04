#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>
#include <vector>
#include <string>
#include <map>

#define SEED_LENGTH 65

const char word_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *word_seed_num = (unsigned short*)word_seed;

const int TAG_BUFFER = 0;

long TASK_SIZE = 256;
char INPUT_PATH[128] = "res/wikipedia_test_small.txt";
char OUTPUT_PATH[128] = "output/results.csv";

int calculateDestRank(char *word, int length, int num_ranks);
int processFlags(int argc, char *argv[]);

typedef struct KeyValue {
  char word[15];
  int count;
} keyvalue;

int Map(char* buf, int len, KeyValue *pair, int *offset, int *word_len);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    // Process flags
    processFlags(argc, argv);

    int rank;
    int num_ranks;
    int iterations;

    int *sizes;
    int *displacements;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    int data_size_per_process;

    MPI::File file;


    if(rank == 0){
      /**
          MASTER:
          -Open file, split and assign (num_ranks - 1) blocks of 64 MB to slaves
          -Do slave work during reduce phaseword
          -Retrieve aggregated data from slave processes and write to file
      */
        file = MPI::File::Open(MPI_COMM_SELF, INPUT_PATH,
            MPI_MODE_RDONLY, MPI_INFO_NULL);

        if(file == MPI_FILE_NULL){
            printf("File does not exist.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        //Checking file_size
        MPI_Offset file_size;
        MPI_File_get_size(file, &file_size);
        printf("[Master] File size %d.\n", (int) file_size);
        //The size of the remaining chunk
        int remainder = (int) file_size % (TASK_SIZE * (num_ranks - 1));
        int chunks_per_task = (file_size - remainder) / ((num_ranks - 1)*TASK_SIZE);

        data_size_per_process = chunks_per_task*TASK_SIZE;

        //prepare variable arrays for call to MPI_Scatterv()
        sizes = new int[num_ranks]();
        displacements = new int[num_ranks]();

        //displacements be like {0, TASK_SIZE, 2*TASK_SIZE, }
        for(int i = 1; i < num_ranks; i++) {
          sizes[i] = TASK_SIZE;
          displacements[i] = (i- 1)*TASK_SIZE;
        }
      }

      //Broadcast the size of the data set per process
      MPI_Bcast(&data_size_per_process, 1, MPI_INT, 0, MPI_COMM_WORLD);

      //Initialize the sizes and displacements arrays
      std::vector<std::map<std::string, int> > bucket(num_ranks);

      while(data_size_per_process > 0) {
        char* buf;

        if(rank == 0) {
          buf = new char[(num_ranks - 1) * TASK_SIZE];
          file.Seek(0, MPI_SEEK_CUR);
          file.Read(buf, (num_ranks - 1) * TASK_SIZE, MPI_CHAR);
        }
        else {
          buf = new char[TASK_SIZE];
        }

        MPI_Scatterv(buf, sizes, displacements, MPI_CHAR, buf, TASK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

        if(rank != 0) {
          int offset = 0;
          while(offset < TASK_SIZE) {
            KeyValue *pair = new KeyValue();

            int word_len = 0;
            int read = Map(buf, TASK_SIZE, pair, &offset, &word_len);

            //if word len
            if(word_len > 0 && read != -1) {
              int destination_rank = calculateDestRank(pair->word, word_len, num_ranks);
              std::string key(pair->word);
              bucket[destination_rank][key] ++;
              //TODO Få med sista H'et
              // std::cout << key << "\n";
            }
            free(pair);
          }
        }
        free(buf);
        data_size_per_process -= TASK_SIZE;
      }

      //Create MPI datatype for KeyValue redistribution
      MPI_Datatype MPI_KeyValue;
      const int number_of_items = 2;
      int b_lengths[2] = {15, 1};
      MPI_Datatype types[2] = {MPI_CHAR, MPI_INT};
      MPI_Aint displacementadores[2] = {offsetof(keyvalue, word), offsetof(keyvalue, count)};
      // std::cout << "displacementadores: {" << displacementadores[0] << ", " << displacementadores[1] << "} \n";


      MPI_Type_create_struct(number_of_items, b_lengths, displacementadores, types, &MPI_KeyValue);
      MPI_Type_commit(&MPI_KeyValue);

      //Array of displacements and sizes for call to alltoallv
      int *s_redistr_sizes =  new int[num_ranks]();
      int *r_redistr_sizes =  new int[num_ranks]();
      int *s_redistr_displs = new int[num_ranks]();
      int *r_redistr_displs = new int[num_ranks]();

      int num_keyvalues_tot = 0;
      //initialize redistributions and sizes
      // if(rank != 0) {
        for(int i = 0; i < num_ranks; i++) {
          s_redistr_sizes[i] = bucket[i].size();
          s_redistr_displs[i] = num_keyvalues_tot;
          num_keyvalues_tot += bucket[i].size();
        }

      MPI_Alltoall(s_redistr_sizes, 1, MPI_INT, r_redistr_sizes, 1, MPI_INT, MPI_COMM_WORLD);

      r_redistr_displs[0] = 0;
      for(int i = 1; i < num_ranks; i++) {
        r_redistr_displs[i] = r_redistr_sizes[i-1] + r_redistr_displs[i-1];
        // std::cout << "SLAVE " << rank << " will have offset " << r_redistr_displs[i] << " from Slave " << i << "\n";
      }

      //Array containing the actual keyvalues
      KeyValue *s_keyvalues = new KeyValue[num_keyvalues_tot]();
      int sub_index = 0;
      for(int i = 0; i < num_ranks; i++) {
        for(std::map<std::string, int>::iterator iter = bucket[i].begin(); iter != bucket[i].end(); ++iter) {
          KeyValue val;
          strcpy(val.word, iter->first.c_str());
          val.count = iter->second;
          s_keyvalues[sub_index] = val;
          // std::cout << "SLAVE " << rank << " will send " << s_keyvalues[sub_index].word << "\n";
          sub_index++;
        }
      }

      //calculate how much data this process will receive in total
      int size_tot = 0;
      for(int i = 0; i < num_ranks; i++) {
        size_tot += r_redistr_sizes[i];
        // std::cout << "SLAVE " << rank << " will get " << r_redistr_sizes[i] << " from " << i << "\n";
      }
      // std::cout << "SLAVE " << rank << " will get " << size_tot << "\n";

      KeyValue *r_keyvalues = new KeyValue[size_tot]();
      // std::cout << "********\n";
      // std::cout << "SLAVE " << rank << " will send " << num_keyvalues_tot << " values in total.\n";
      // std::cout << "SLAVE " << rank << " will receive " << size_tot << " values in total.\n";
      // std::cout << "********\n";


      MPI_Alltoallv(s_keyvalues, s_redistr_sizes, s_redistr_displs,
        MPI_KeyValue, r_keyvalues, r_redistr_sizes, r_redistr_displs,
        MPI_KeyValue, MPI_COMM_WORLD);


      //REDUCE
      for(int i = 0; i < size_tot; i++) {
        std::cout << "Slave " << rank << " received " << r_keyvalues[i].word << ", count " << r_keyvalues[i].count << "\n";
      }

    free(s_keyvalues);

    //TODO check out
    free(r_keyvalues);

    free(s_redistr_sizes);
    free(r_redistr_sizes);
    free(s_redistr_displs);
    free(r_redistr_displs);

    if(rank == 0) {
      free(sizes);
      free(displacements);
    }
    // else {
    //   for(int i = 0; i < num_ranks; i++) {
    //     for(std::map<std::string, int>::iterator iter = bucket[i].begin(); iter != bucket[i].end(); ++iter) {
    //         printf("[Slave %d] asdsad asd ", rank);
    //         std::cout << " Word " << iter->first << ", Count " <<iter->second << "\n";
    //     }
    //   }
    // }

    // printf("[Process %d] Exiting.\n", rank);
    MPI_Finalize();
    return 0;
}

int calculateDestRank(char *word, int length, int num_ranks)
{
    uint hash = 0;

    for (uint i = 0; i < length; i++)
    {
        uint num_char = (uint)word[i];
        uint seed     = (uint)word_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return (int)(hash % (uint)num_ranks);
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

int Map(char* buf, int len, KeyValue *pair, int *offset, int *word_len) {
  //Eat up all the numbers in the front
  while(!isalpha(buf[*offset]) && !isdigit(buf[*offset])) {
     ++*offset;
  }

  int offset_start = *offset;
  int max_word_len = 15;

   if(*offset >= TASK_SIZE) return -1;

  if(isalpha(buf[*offset])) {
    while(isalpha(buf[*offset])) {
      pair->word[*word_len] = buf[*offset];
      ++*word_len;
      ++*offset;
      if(*word_len > max_word_len - 1  || *offset >= TASK_SIZE) {
        break;
      }
    }
  }
  else if(isdigit(buf[*offset])) {
    while(isdigit(buf[*offset])) {
      pair->word[*word_len] = buf[*offset];
      ++*word_len;
      ++*offset;
      if(*word_len > max_word_len - 1 || *offset >= TASK_SIZE) {
        break;
      }
    }
  }

  if(*word_len == 0) return -1;

  pair->count = 1;

  pair->word[max_word_len - 1] = '\0';
  return *offset - offset_start;
}

int Reduce(KeyValue *k1, KeyValue *k2) {

  k1->count += k2->count;
  return 1;
}
