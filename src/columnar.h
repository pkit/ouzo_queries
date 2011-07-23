/**
 * Copyright 2011, BigDataCraft.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * columnar.h
 *
 *  Created on: May 31, 2011
 *      Author: nhsan
 */

#ifndef COLUMNAR_H_
#define COLUMNAR_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

char BYTE_NULL = 0;
int32_t INT_NULL = 0;
int64_t LONG_NULL = 0;
float FLOAT_NULL = 0.0;
double DOUBLE_NULL = 0.0;
char* STRING_NULL = "";

#define COUNTRY_MAX_REP			0
#define COUNTRY_MAX_DEF			1
#define AGENT_MAX_REP			0
#define AGENT_MAX_DEF			1
#define REQUEST_ID_MAX_REP		1
#define REQUEST_ID_MAX_DEF		1
#define REQUEST_URL_MAX_REP		1
#define REQUEST_URL_MAX_DEF		1
#define REQUEST_TIME_MAX_REP	1
#define REQUEST_TIME_MAX_DEF	1
#define SESSION_ID_MAX_REP		0
#define SESSION_ID_MAX_DEF		0

typedef union
{
	char* _byte_ptr;
	int32_t* _int_ptr;
	int64_t* _long_ptr;
	float* _float_ptr;
	double* _double_ptr;
	char* _string_ptr;
} data_pointer;

typedef struct _columnar
{
	char* level_buf;
	char* data_buf;

	char* level_ptr;
	data_pointer data_ptr;
	char* level_end_buf;

	int level_size;
	int data_size;
} columnar;

#define COLUMNAR_DECLARE(name,base)\
	columnar	name;\
	columnar_open(&name, base);\
	datasize += columnar_size(name);\
	columnar_init(name);

#define COLUMNAR_COUNT_DECLARE(name,base)\
	columnar	name;\
	columnar_open_for_count_only(&name, base);\
	datasize += columnar_size(name);\
	columnar_init(name);

#define COMMON_VARIABLES_DECLARE\
	int has_more_slice = 1;\
	int next_level = 0;\
	int fetch_level = 0;\
	int select_level = 0;\
	clock_t start_time, end_time;\
	int64_t datasize = 0;

#define columnar_open(col,base_file_name)					{columnar_level_open(col,base_file_name); columnar_data_open(col,base_file_name);}
#define columnar_open_for_count_only(col,base_file_name)	{columnar_level_open(col,base_file_name);}
#define columnar_init(col)									{col.level_ptr = col.level_buf;col.data_ptr._byte_ptr = col.data_buf;}
#define columnar_size(col)									(col.data_size + col.level_size)
#define columnar_eof(col)									(col.level_ptr==col.level_end_buf)
#define str_hash(s)											(*(((size_t*) s) - 1))
#define str_length(s)										(*(((size_t*) s) - 2))

static inline bool str_compare(const char* s1, const char* s2)
{
	if (s1 == s2) return true;
	register const unsigned long* p1 = (unsigned long*) (s1);
	register const unsigned long* p2 = (unsigned long*) (s2);
	register const unsigned long* e1 = p1 + ((*(p1 - 2)) >> 2) + 1;

	for (; *p1 == *p2; ++p1, ++p2)
	{
		if (p1 <= e1) return true;
	}
	return false;
}

static inline bool str_compare2(const char* s1, const char* s2)
{
	if (s1 == s2) return true;
	for (; *s1 == *s2; ++s1, ++s2)
		if (*s1 == 0) return true;
	return false;
}

#define fetch_str(col, max_def)\
{\
	if (!columnar_eof(col))\
	{\
		has_more_slice = 1;\
		if (col.level_ptr[0] >= fetch_level)\
		{\
			if (col.level_ptr[1] < max_def) /*is null*/\
			{\
				process_null_for_##col(STRING_NULL);\
			}\
			else\
			{\
				int len = *col.data_ptr._int_ptr++;\
				col.data_ptr._int_ptr++;\
				process_value_for_##col(col.data_ptr._byte_ptr);\
				len++;\
				len += len % 4;\
				col.data_ptr._byte_ptr += len;\
			}\
			col.level_ptr += 2;\
		}\
	}\
	if ((!columnar_eof(col))&&(next_level < col.level_ptr[0])) next_level = col.level_ptr[0];\
}

#define fetch_count_only(col, max_def)\
{\
	if (!columnar_eof(col))\
	{\
		has_more_slice = 1;\
		if (col.level_ptr[0] >= fetch_level)\
		{\
			if (col.level_ptr[1] >= max_def) /*not null*/\
			{\
				process_count_for_##col();\
			}\
			col.level_ptr += 2;\
		}\
	}\
	if ((!columnar_eof(col))&&(next_level < col.level_ptr[0])) next_level = col.level_ptr[0];\
}

void columnar_level_open(columnar* col, char* base_file_name)
{
	int fd;
	struct stat statbuf;
	char tmp[1024];

	/**************OPEN LEVEL FILE*****************************/
	/* open the input file */
	sprintf(tmp, "%s.level", base_file_name);
	if ((fd = open(tmp, O_RDONLY)) < 0)
	{
		perror("Can't open level file for reading");
		exit(EXIT_FAILURE);
	}

	/* find size of input file */
	if (fstat(fd, &statbuf) < 0)
	{
		perror("fstat error");
		exit(EXIT_FAILURE);
	}

	col->level_size = statbuf.st_size;

	/* mmap the input file */
	col->level_buf = (char*) mmap(0, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (col->level_buf == MAP_FAILED)
	{
		perror("mmap error for level file");
		exit(EXIT_FAILURE);
	}

	col->level_end_buf = col->level_buf + statbuf.st_size;

	if (close(fd) == -1)
	{
		perror("Error in close level file");
		exit(EXIT_FAILURE);
	}

	col->data_size=0;
	col->data_buf = NULL;
}

void columnar_data_open(columnar* col, char* base_file_name)
{
	int fd;
	struct stat statbuf;
	char tmp[1024];
	/**************OPEN DATA FILE*****************************/
	/* open the input file */
	sprintf(tmp, "%s.dremel", base_file_name);
	if ((fd = open(tmp, O_RDONLY)) < 0)
	{
		perror("Can't open data file for reading");
		exit(EXIT_FAILURE);
	}

	/* find size of input file */
	if (fstat(fd, &statbuf) < 0)
	{
		perror("fstat error");
		exit(EXIT_FAILURE);
	}
	col->data_size = statbuf.st_size;

	/* mmap the input file */
	col->data_buf = (char*) mmap(0, statbuf.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (col->data_buf == MAP_FAILED)
	{
		perror("mmap error for level file");
		exit(EXIT_FAILURE);
	}

	if (close(fd) == -1)
	{
		perror("Error in close data file");
		exit(EXIT_FAILURE);
	}
}

void columnar_close(columnar* col)
{
	if (munmap(col->level_buf, col->level_size) == -1) perror("munmap level file");
	if (col->data_buf)
	{
		if (munmap(col->data_buf, col->data_size) == -1) perror("munmap data file");
	}
}

#endif /* COLUMNAR_H_ */
