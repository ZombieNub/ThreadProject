#include <stdio.h>     
#include <stdlib.h>   
#include <stdint.h>  
#include <inttypes.h>  
#include <errno.h>     // for EINTR
#include <fcntl.h>     
#include <unistd.h>    
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "common.h"
#include "common_threads.h"

typedef struct HashData {
  uint8_t* full_text;
  uint32_t nblocks, thread_number, max_threads;
} HashData;

// Print out the usage of the program and exit.
void Usage(char*);
uint32_t jenkins_one_at_a_time_hash(const uint8_t* , uint64_t );
void* hash_thread(void* );
uint32_t hash_wrapper(HashData );

// block size
#define BSIZE 4096

int 
main(int argc, char** argv) 
{
  int32_t fd;
  uint32_t hash, max_threads;
  uint64_t nblocks, file_size;
  struct stat file_stats;
  uint8_t *arr;

  pthread_t head_thread;
  HashData head_data;

  // input checking 
  if (argc != 3)
    Usage(argv[0]);
  // Assuming the second argument is a number, convert it to a number and set it to max_threads
  max_threads = atoi(argv[2]);

  // open input file
  fd = open(argv[1], O_RDWR);
  if (fd == -1) {
    perror("open failed");
    exit(EXIT_FAILURE);
  }
  // use fstat to get file size
  // While unlikely, getting the file stats of the fd might fail, so we should check for it
  if (fstat(fd, &file_stats) == -1) {
    perror("Could not get file information");
    exit(EXIT_FAILURE);
  }
  // Get the file size and store it elsewhere
  file_size = file_stats.st_size;
  // calculate nblocks 
  nblocks = file_size/BSIZE;
  //printf("(Main) File Size: %lu, Block count: %lu\n", file_size, nblocks);
  
  printf(" no. of blocks = %lu \n", nblocks);
  // Load the file information into memory
  // This is put outside of the GetTime calls,
  // because we don't want to determine how long it took to load the memory
  // AND process the hash function. We only want to measure the latter
  arr = (uint8_t*)mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
  //printf("%lu\n", (uint64_t)arr);
  //printf("%p: %u\n", arr + BSIZE, arr[BSIZE]);
  // Now we need to fill out the head information
  head_data.full_text = arr;
  head_data.nblocks = nblocks;
  head_data.thread_number = 0;
  head_data.max_threads = max_threads;
  // Now we can get the hash

  double start = GetTime();

  // calculate hash value of the input file
  Pthread_create(&head_thread, NULL, hash_thread, &head_data);
  void* hash_return = (void*)malloc(sizeof(uint32_t));
  Pthread_join(head_thread, &hash_return);
  hash = *(uint32_t*)hash_return;

  double end = GetTime();
  printf("hash value = %u \n", hash);
  printf("time taken = %f \n", (end - start));
  // Free hash_return since we no longer need it
  free(hash_return);
  close(fd);
  return EXIT_SUCCESS;
}

void* hash_thread(void* hash_data_input) {
  HashData data = *(HashData*)hash_data_input;
  // Cleanup required, do not forget to free after returning
  uint32_t* hash_ptr = (uint32_t*)malloc(sizeof(uint32_t));
  if ((data.thread_number * 2) + 2 < data.max_threads) {
    // We can spawn both a left and right child
    //printf("LR thread!\n");
    pthread_t left, right;
    HashData left_data, right_data;
    uint32_t* left_hash_ptr, *right_hash_ptr;
    uint32_t center_hash;
    // The jenkins hash function always returns 8 chars, except this may expand to 10 digits when interpreted as decimals,
    // so we need to allocate 31 chars
    // calloc is used to guarantee there will be null characters after the string
    char* new_hash = (char*)malloc(sizeof(char) * 31);
    // We first need to construct two new pieces of data, one for the left hash and one for the right hash
    left_data.full_text = data.full_text; right_data.full_text = data.full_text;
    left_data.max_threads = data.max_threads; right_data.max_threads = data.max_threads;
    left_data.nblocks = data.nblocks; right_data.nblocks = data.nblocks;
    // Designate the proper thread numbers for block selection and filling out the complete tree
    left_data.thread_number = (data.thread_number * 2) + 1;
    right_data.thread_number = (data.thread_number * 2) + 2;
    // Now we can call the two threads and give them the relevant information.
    Pthread_create(&left, NULL, hash_thread, &left_data);
    Pthread_create(&right, NULL, hash_thread, &right_data);
    // While we are waiting on the threads to complete, we will evaluate our center hash
    center_hash = hash_wrapper(data);
    // Now we need to join the threads and get their relevant hashes.
    Pthread_join(left, (void**)&left_hash_ptr);
		Pthread_join(right, (void**)&right_hash_ptr);
    // Now we need to join these values into a single string and evaluate its hash
    uint8_t new_length = sprintf(new_hash, "%u%u%u", center_hash, *left_hash_ptr, *right_hash_ptr);
    // Free left_hash_ptr and right_hash_ptr, since they were created using malloc
    free(left_hash_ptr);
    free(right_hash_ptr);
    // char is the same as uint8_t
    *hash_ptr = jenkins_one_at_a_time_hash((uint8_t*)new_hash, new_length);
    // Free new_hash, since it's not needed anymore
    free(new_hash);
    pthread_exit(hash_ptr);
  } else if ((data.thread_number * 2) + 1 < data.max_threads) {
    // We can only spawn a left child
    //printf("L thread!\n");
    pthread_t left;
    HashData left_data;
    uint32_t* left_hash_ptr;
    uint32_t center_hash;
    // We need to allocate 21
    char* new_hash = (char*)malloc(sizeof(char) * 21);
    // We first need to construct data only for the left child this time
    left_data.full_text = data.full_text;
    left_data.max_threads = data.max_threads;
    left_data.nblocks = data.nblocks;
    // Designate the proper thread numbers for block selection and filling out the complete tree
    left_data.thread_number = (data.thread_number * 2) + 1;
    // Now we can call the single threads and give it the relevant information.
    Pthread_create(&left, NULL, hash_thread, &left_data);
    // While we are waiting on the thread to complete, we will evaluate our center hash
    center_hash = hash_wrapper(data);
    // Now we need to join the threads and get their relevant hashes.
    Pthread_join(left, (void**)&left_hash_ptr);
    // Now we need to join these values into a single string and evaluate its hash
    uint8_t new_length = sprintf(new_hash, "%u%u", center_hash, *left_hash_ptr);
    // Free left_hash_ptr, since it was created using malloc
    free(left_hash_ptr);
    // char is the same as uint8_t
    *hash_ptr = jenkins_one_at_a_time_hash((uint8_t*)new_hash, new_length);
    // Again, free new-hash since we don't need it
    free(new_hash);
    pthread_exit(hash_ptr);
  } else {
    // We cannot spawn any children, so we are simply a leaf!
    //printf("Leaf thread!\n"); // DEBUG: Get rid of this before submission
    *hash_ptr = hash_wrapper(data);
    pthread_exit(hash_ptr);
  }
  return NULL; // We shouldn't reach this point, but still doing this for completeness's sake
}

uint32_t hash_wrapper(HashData data) {
  uint64_t block_amount = data.nblocks / data.max_threads;
  uint64_t start = data.thread_number * block_amount;
  //printf("(Hash), Block amount: %lu, Start point: %lu\n", block_amount, start);
  return jenkins_one_at_a_time_hash(data.full_text + (start * BSIZE), block_amount * BSIZE);
}

uint32_t 
jenkins_one_at_a_time_hash(const uint8_t* key, uint64_t length) 
{
  uint64_t i = 0;
  uint32_t hash = 0;

  while (i != length) {
    hash += key[i++];
    hash += hash << 10;
    hash ^= hash >> 6;
  }
  hash += hash << 3;
  hash ^= hash >> 11;
  hash += hash << 15;
  return hash;
}


void 
Usage(char* s) 
{
  fprintf(stderr, "Usage: %s filename num_threads \n", s);
  exit(EXIT_FAILURE);
}
