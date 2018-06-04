#include "mapreduce.h"

int main(int argc, char *argv[])
{
    // Process flags
    processFlags(argc, argv);

    MPI_Init(&argc, &argv);
    // For timing
    double t1, t2;

    // For communication
    int rank, num_ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_ranks);

    MPI::File file;
    if(rank == 0){
      t1 = mysecond();
    }
    file = MPI::File::Open(MPI_COMM_WORLD, INPUT_PATH,
        MPI_MODE_RDONLY, MPI_INFO_NULL);
    if(!file){
        printf("File does not exist.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    //Checking file_size
    MPI::Offset file_size = file.Get_size();
    // Calculate data to be processed if not multiple of num_ranks the remainder is ignored
    int remainder = file_size % (TASK_SIZE * (num_ranks));
    int chunks_per_task = (file_size - remainder) / ((num_ranks)*TASK_SIZE);

    long data_size_per_process = chunks_per_task*TASK_SIZE;

    // Read input and put it in bucket.
    std::vector<std::map<std::string, uint64_t> > bucket(num_ranks);
    file.Seek(rank * data_size_per_process, MPI_SEEK_SET);
    while(data_size_per_process > 0) {
      char *buf = new char[TASK_SIZE];
      file.Read_all(buf, TASK_SIZE, MPI_CHAR);

        int offset = 0;
        while(offset < TASK_SIZE) {
          KeyValue *pair = new KeyValue();

          int word_len = 0;
          int read = Map(buf, TASK_SIZE, pair, &offset, &word_len);

          if(word_len > 0 && read != -1) {
            int destination_rank = calculateDestRank(pair->word, word_len, num_ranks);
            std::string key(pair->word);
            bucket[destination_rank][key] ++;
            }
          delete pair;
        }
      delete buf;
      data_size_per_process -= TASK_SIZE;

      }
      file.Close();

    //Create MPI datatype for KeyValue redistribution
    MPI_Datatype MPI_KeyValue;
    Create_KeyValue_Datatype(&MPI_KeyValue);

    //Array of displacements and sizes for call to alltoallv
    int *s_redistr_sizes =  new int[num_ranks]();
    int *r_redistr_sizes =  new int[num_ranks]();
    int *s_redistr_displs = new int[num_ranks]();
    int *r_redistr_displs = new int[num_ranks]();
    uint64_t num_keyvalues_tot = 0;
    Calculate_SendRecv_Count_And_Displs(bucket, s_redistr_sizes, r_redistr_sizes,
      s_redistr_displs,r_redistr_displs, num_keyvalues_tot, num_ranks);

    //Array containing the actual keyvalues
    KeyValue *s_keyvalues = new KeyValue[num_keyvalues_tot]();
    int sub_index = 0;
    for(int i = 0; i < num_ranks; i++) {
      for(std::map<std::string, uint64_t>::iterator iter = bucket[i].begin(); iter != bucket[i].end(); ++iter) {
        KeyValue val;
        strcpy(val.word, iter->first.c_str());
        val.count = iter->second;
        s_keyvalues[sub_index] = val;
        sub_index++;
      }
    }

    //calculate how much data this process will receive in total
    int size_tot = 0;
    for(int i = 0; i < num_ranks; i++) {
      size_tot += r_redistr_sizes[i];
    }

    KeyValue *r_keyvalues = new KeyValue[size_tot]();

    MPI_Alltoallv(s_keyvalues, s_redistr_sizes, s_redistr_displs,
      MPI_KeyValue, r_keyvalues, r_redistr_sizes, r_redistr_displs,
      MPI_KeyValue, MPI_COMM_WORLD);

    delete s_redistr_sizes;
    delete r_redistr_sizes;
    delete s_redistr_displs;
    delete r_redistr_displs;
    delete s_keyvalues;

    //Reduce phase
    std::map<std::string, uint64_t> reducedKeyValues;
    for(uint64_t i = 0; i < size_tot; i++) {
      reducedKeyValues[std::string(r_keyvalues[i].word)] += r_keyvalues[i].count;
    }
    delete r_keyvalues;

    //Combine phase
    int size_of_aggregation = reducedKeyValues.size();
    int *sizes_of_aggregation = new int[num_ranks]();

    MPI_Gather(&size_of_aggregation, 1, MPI_INT, sizes_of_aggregation, 1, MPI_INT, 0, MPI_COMM_WORLD);

    //Prepare keyvalues to be gathered by master
    KeyValue *s_aggregated_keyvalues = new KeyValue[size_of_aggregation]();
    Reduce(reducedKeyValues, s_aggregated_keyvalues);

    // Prepare sizes and displacement for the gatherv
    KeyValue *r_aggregated_keyvalues;
    int tot_size_aggr = 0;
    int *displs_aggr;
    if(rank == 0){
      displs_aggr = new int[num_ranks]();
      for(int i = 0; i < num_ranks; i++){
        displs_aggr[i] = tot_size_aggr;
        tot_size_aggr += sizes_of_aggregation[i];
      }
      r_aggregated_keyvalues = new KeyValue[tot_size_aggr]();
    }

    MPI_Gatherv(s_aggregated_keyvalues, size_of_aggregation, MPI_KeyValue,
      r_aggregated_keyvalues, sizes_of_aggregation, displs_aggr, MPI_KeyValue, 0, MPI_COMM_WORLD);

    delete s_aggregated_keyvalues;

    // Save result in OUTPUT_PATH
    if(rank == 0) {
      MPI::File output_file = MPI::File::Open(MPI_COMM_SELF, OUTPUT_PATH,
            MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL);
      for(int i = 0; i < tot_size_aggr; i++) {
          char output_buf[50];
          sprintf(output_buf, "%s, %"PRIu64"\n", r_aggregated_keyvalues[i].word, r_aggregated_keyvalues[i].count);
          output_file.Write(output_buf, strlen(output_buf), MPI_CHAR);
      }
      output_file.Close();
    }

    //Free up some space and print the execution time
    if(rank == 0) {
      delete sizes_of_aggregation;
      delete r_aggregated_keyvalues;
      delete displs_aggr;
      double time = 0;
      t2 = mysecond();
      time = t2-t1;
      std::cout << "Execution time: " << time << "s\n";
    }
    MPI_Finalize();
    return 0;
}

int calculateDestRank(char *word, int length, int num_ranks){
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
    while(!isalpha(buf[*offset]) && !isdigit(buf[*offset])) {
       ++*offset;
    }

    int offset_start = *offset;

     if(*offset >= TASK_SIZE) return -1;

    if(isalpha(buf[*offset])) {
      while(isalpha(buf[*offset])) {
        pair->word[*word_len] = buf[*offset];
        ++*word_len;
        ++*offset;
        if(*word_len > WORD_LENGTH - 1  || *offset >= TASK_SIZE) {
          break;
        }
      }
    }
    else if(isdigit(buf[*offset])) {
      while(isdigit(buf[*offset])) {
        pair->word[*word_len] = buf[*offset];
        ++*word_len;
        ++*offset;
        if(*word_len > WORD_LENGTH - 1 || *offset >= TASK_SIZE) {
          break;
        }
      }
    }
    if(*word_len == 0) return -1;

    pair->word[WORD_LENGTH - 1] = '\0';
    return *offset - offset_start;
}

int Reduce(std::map<std::string, uint64_t> &reducedKeyValues, KeyValue *s_aggregated_keyvalues) {
    int sub_index = 0;
    for(std::map<std::string, uint64_t>::iterator iter = reducedKeyValues.begin(); iter != reducedKeyValues.end(); ++iter) {
      strcpy(s_aggregated_keyvalues[sub_index].word, iter->first.c_str());
      s_aggregated_keyvalues[sub_index].count = iter->second;
      sub_index ++;
    }
    return 1;
}

//Taken from lab 1
double mysecond(){
    struct timeval tp;
    struct timezone tzp;
    int i;
    i = gettimeofday(&tp,&tzp);
    return ((double) tp.tv_sec + (double) tp.tv_usec * 1.e-6);
}

int Create_KeyValue_Datatype(MPI_Datatype *MPI_KeyValue){
    const int number_of_items = 2;
    int b_lengths[2] = {WORD_LENGTH, 1};
    MPI_Datatype types[2] = {MPI_CHAR, MPI_UINT64_T};
    MPI_Aint displacementadores[2] = {offsetof(keyvalue, word), offsetof(keyvalue, count)};

    MPI_Type_create_struct(number_of_items, b_lengths, displacementadores, types, MPI_KeyValue);
    MPI_Type_commit(MPI_KeyValue);
    return 1;
}

int Calculate_SendRecv_Count_And_Displs(std::vector<std::map<std::string, uint64_t> > &bucket, int *s_redistr_sizes,
  int *r_redistr_sizes, int *s_redistr_displs,int *r_redistr_displs, uint64_t &num_keyvalues_tot, const int num_ranks){
    for(int i = 0; i < num_ranks; i++) {
      s_redistr_sizes[i] = bucket[i].size();
      s_redistr_displs[i] = num_keyvalues_tot;
      num_keyvalues_tot += bucket[i].size();
    }

    MPI_Alltoall(s_redistr_sizes, 1, MPI_INT, r_redistr_sizes, 1, MPI_INT, MPI_COMM_WORLD);

    r_redistr_displs[0] = 0;
    for(int i = 1; i < num_ranks; i++) {
      r_redistr_displs[i] = r_redistr_sizes[i-1] + r_redistr_displs[i-1];
    }

    return 1;
}
