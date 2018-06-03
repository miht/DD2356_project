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
  int64_t count;
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
            // MPI_Finalize();
            // exit(0);
        }
        else {
          printf("File opened!");
        }

        //Checking file_size
        MPI_Offset file_size;
        MPI_File_get_size(file, &file_size);
        printf("[Master] File size %d.\n", (int) file_size);
        //The size of the remaining chunk
        int remainder = (int) file_size % (TASK_SIZE * (num_ranks - 1));
        int chunks_per_task = (file_size - remainder) / ((num_ranks - 1)*TASK_SIZE);
        printf("CPT %d\n", chunks_per_task);

        data_size_per_process = chunks_per_task*TASK_SIZE;
        printf("DSPP %d\n", data_size_per_process);



        //prepare variable arrays for call to MPI_Scatterv()
        sizes = new int[num_ranks]();
        // sizes = (int*) calloc(num_ranks, sizeof(int));
        displacements = new int[num_ranks]();
        // displacements = (int*) calloc(num_ranks, sizeof(int));

        //Sizes be like {0, 256, 256, 256, 256.....}; master won't receive any
        //displacements be like {256, }
        for(int i = 1; i < num_ranks; i++) {
          sizes[i] = TASK_SIZE;
          displacements[i] = (i- 1)*TASK_SIZE;
        }

      }
      // MPI_Bcast(&data_per_process, 1, MPI_INT, 0, MPI_COMM_WORLD);
      //Broadcast the size of the data set per process
      MPI_Bcast(&data_size_per_process, 1, MPI_INT, 0, MPI_COMM_WORLD);

      //Initialize the sizes and displacements arrays

      std::vector<std::map<std::string, int64_t> > bucket(num_ranks);

      while(data_size_per_process > 0) {
        char* buf;

        if(rank == 0) {
          buf = new char[(num_ranks - 1) * TASK_SIZE];
          // buf =  (char*) calloc((num_ranks - 1) * TASK_SIZE, sizeof(char));
          file.Seek(0, MPI_SEEK_CUR);
          // MPI_File_read(file, buf, (num_ranks - 1)*TASK_SIZE, MPI_CHAR, MPI_STATUS_IGNORE);

          file.Read(buf, (num_ranks - 1) * TASK_SIZE, MPI_CHAR);
          printf("[Master] Read %li bytes of data.\n", (num_ranks - 1) * TASK_SIZE);
        }
        else {
          //  buf = (char*) calloc(TASK_SIZE, sizeof(char));
          buf = new char[TASK_SIZE];
        }

        MPI_Scatterv(buf, sizes, displacements, MPI_CHAR, buf, TASK_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);

        if(rank != 0) {
          printf("[Slave %d] Received chunk of size: %li\n", rank, TASK_SIZE);
          // printf("[Slave %d] Has chunk %s: \n", rank, buf);

          int offset = 0;
          while(offset < TASK_SIZE) {
            KeyValue *pair;
            // pair = calloc(1, sizeof(pair));
            pair = new KeyValue();
            // pair.word = (char*) calloc(KeyValue_word_LENGTH, sizeof(char));
            // printf("%s\n", buf);
            int word_len = 0;
            int read = Map(buf, TASK_SIZE, pair, &offset, &word_len);

            //if word len
            if(word_len > 0) {
              int destination_rank = calculateDestRank(pair->word, word_len, num_ranks);
              // std::cout << pair->word << "\n"
              std::string key(pair->word);
              // if(bucket[destination_rank].count(key) < 1) bucket[destination_rank].insert(std::make_pair<std::string, int64_t>(key, 0));
              bucket[destination_rank][key] ++;
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
      // }

      // std::cout << s_redistr_sizes << "\n";

      MPI_Alltoall(s_redistr_sizes, 1, MPI_INT, r_redistr_sizes, 1, MPI_INT, MPI_COMM_WORLD);
      //MPI_Alltoall(s_redistr_displs, 1, MPI_INT, r_redistr_displs, 1, MPI_INT, MPI_COMM_WORLD);

      r_redistr_displs[0] = 0;
      for(int i = 1; i < num_ranks; i++){
        r_redistr_displs[i] = r_redistr_sizes[i-1] + r_redistr_displs[i-1];
      }

      // for(int i = 0; i < num_ranks; i++) {
      //   // printf("[Rank %d] ******* BALLE %d*********\n", rank, r_redistr_sizes[i]);
      //   printf("[Rank %d] ******* BALLE %d*********\n", rank, r_redistr_sizes[i]);
      //   printf("[Rank %d] ******* BALLE %d*********\n", rank, r_redistr_displs[i]);
      // }

      //Array containing the actual keyvalues
      // KeyValue s_keyvalues[num_keyvalues_tot];
      KeyValue *s_keyvalues = new KeyValue[num_keyvalues_tot]();
      //increment variable for adding keyvalues
      int sub_index = 0;
      for(int i = 0; i < num_ranks; i++) {

        for(std::map<std::string, int64_t>::iterator iter = bucket[i].begin(); iter != bucket[i].end(); ++iter) {
          KeyValue val;
          // val.word = iter->first.c_str();
          strcpy(val.word, iter->first.c_str());
          val.count = iter->second;
          s_keyvalues[sub_index] = val;
          sub_index++;
          // std::cout << "DOH " << val.word << ", "<< val.count << "\n";
        }
      }

      // std::cout << "num_keyvalues_tot " << num_keyvalues_tot << ", sub_index " << sub_index << "\n";

      //calculate how much data this process will receive in total
      int size_tot = 0;
      for(int i = 0; i < num_ranks; i++) {
        size_tot += r_redistr_sizes[i];
      }

      std::cout << "size_tot" << size_tot << "\n";

      KeyValue *r_keyvalues = new KeyValue[size_tot]();

      MPI_Alltoallv(s_keyvalues, s_redistr_sizes, s_redistr_displs,
        MPI_KeyValue, r_keyvalues, r_redistr_sizes, r_redistr_displs,
        MPI_KeyValue, MPI_COMM_WORLD);
      //Alltoallv

      // std::cout << "Testing (___)|||||||||||||||||||||||||||||||D \n";

      //REDUCE
        for(int i = 0; i < size_tot; i++) {
          std::cout << "Slave " << rank << " received " << r_keyvalues[i].word << ", count " << r_keyvalues[i].count << "\n";
        }



      //TODO check out
      free(s_keyvalues);


      //TODO check out
      free(r_keyvalues);

    /**
    SLAVE:
        -Retrieve 64 MB chunk from master
        -Until chunk is consumed, perform Map() function, storing results locally
            -The master process provides slaves with way to detect end of file
        -Redistribute all <word, count> pairs across all processes. This is done
         by calling calculateDestRank() to find out rank of receiving process.
        -After receiving redistributed <word, count> pairs, the process aggregates
         the result by calling reduce() repeatedly.
    */

    printf("[Slave %d] Exiting.\n", rank);

    free(s_redistr_sizes);
    free(r_redistr_sizes);
    free(s_redistr_displs);
    free(r_redistr_displs);

    if(rank == 0) {
      free(sizes);
      free(displacements);
    }
    else {

      // free(s_keyvalues;
      // for(int i = 0; i < num_ranks; i++) {
      //   for(std::map<std::string, int64_t>::iterator iter = bucket[i].begin(); iter != bucket[i].end(); ++iter) {
      //       printf("[Slave %d]", rank);
      //       std::cout << " Word " << iter->first << ", Count " <<iter->second << "\n";
      //   }
      // }
    }

    // free(r_keyvalues;


    MPI_Finalize();
    return 0;
}

int calculateDestRank(char *word, int length, int num_ranks)
{
    uint64_t hash = 0;

    for (uint64_t i = 0; i < length; i++)
    {
        uint64_t num_char = (uint64_t)word[i];
        uint64_t seed     = (uint64_t)word_seed_num[(i % SEED_LENGTH)];

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


int Map(char* buf, int len, KeyValue *pair, int *offset, int *word_len) {
  //Eat up all the numbers in the front
  // printf("buf[0] = %c\n", buf[*offset]);
  //
  // printf("IS ALPHA? %d\n", isalpha(buf[*offset]));
  // printf("IS DIGIT? %d\n", isdigit(buf[*offset]));

  while(!isalpha(buf[*offset]) && !isdigit(buf[*offset])) {
     ++*offset;
  }

  int offset_start = *offset;
  int max_word_len = sizeof(pair->word)/sizeof(char) - 1;

  if(isalpha(buf[*offset])) {
    while(isalpha(buf[*offset])) {
      // printf("buf[%d] = %c\n", *offset, buf[*offset]);
      pair->word[*word_len] = buf[*offset];
      ++*word_len;
      ++*offset;
      if(*word_len > max_word_len) {
        break;
      }
    }
  }
  else if(isdigit(buf[*offset])) {
    while(isdigit(buf[*offset])) {
      pair->word[*word_len] = buf[*offset];
      ++*word_len;
      ++*offset;
      if(*word_len > max_word_len) {
        break;
      }
    }
  }

  if(*word_len == 0) return -1;

  // printf("%s\n", pair->word);
  // sprintf(pair->word, "%.*s", word_len, buf - word_len);
  // pair->word[]
  pair->count = 1;

  pair->word[max_word_len - 1] = '\0';
  return *offset - offset_start;
}

int Reduce(KeyValue *k1, KeyValue *k2) {

  k1->count += k2->count;
  return 1;
}
