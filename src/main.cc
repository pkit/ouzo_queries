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
 * @author: NGUYEN Hong San
 * Implement for query Q8:
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

#include "q8.h"
#include "q11.h"
#include "q5.h"
#include "q4.h"
#include "q12.h"

#define POOL_SIZE	1024*1024*128
void* output_pool;

int main(int argc, char* argv[])
{
	if (argc != 4)
	{
		printf("Wrong parameter\n");
		return 0;
	}

	int q = atoi(argv[1]);
	char* path = argv[2];
	char* outfile = argv[3];

	output_pool = malloc(POOL_SIZE);

	switch (q)
	{
	case 4:
		q4(path, outfile);
		break;
	case 5:
		q5(path, outfile);
		break;
	case 8:
		q8(path, outfile);
		break;
	case 11:
		q11(path, outfile);
		break;
	case 12:
		q12(path, outfile);
		break;
	default:
		printf("Wrong query number\n");
		break;
	}
	free(output_pool);
	return 0;
}
