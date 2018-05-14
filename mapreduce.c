#include <stdio.h>
#include <stdint.h>

#define SEED_LENGTH 65

const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

int calculateDestRank(char *word, int length, int num_ranks);

int main(int argc, char *argv[])
{
   // printf() displays the string inside quotation
   printf("Hello, World!\n");
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
