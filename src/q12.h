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
 * Implement for query Q12:
 * ***********************************************************************
 * SELECT
 * 		Requests.URL
 * FROM
 * 		Document
 * WHERE
 * 		Requests.URL like '%ouzzo.com%'
 * ***********************************************************************
 */

#ifndef Q12_H_
#define Q12_H_
#include "columnar.h"

static inline bool check(char* s, int len)
{
	for (int i = 0; i < len - 8; i++) //8 is length of "ouzo.com"
	{
		if (s[i] == 'o' && s[i + 1] == 'u' && s[i + 2] == 'z' && s[i + 3] == 'o' && s[i + 4] == '.' && s[i + 5] == 'c' && s[i + 6] == 'o' && s[i + 7] == 'm')
		{
			return true;
		}
	}
	return false;
}

void q12(char* path, char* outfile)
{
	char tmp_path[1024];
	COMMON_VARIABLES_DECLARE;
	char* result[1000000];
	int result_count = 0;

	/* output */
	char* url_out;
	sprintf(tmp_path, "%s/%s", path, "Document.Requests.URL");
	COLUMNAR_DECLARE(request_url, tmp_path);
	result_count = 0;

	start_time = clock();
	while (1)
	{
		next_level = 0;
		has_more_slice = 0;
		/* fetch request_url */
		if (!columnar_eof(request_url))
		{
			has_more_slice = 1;
			if (request_url.level_ptr[0] >= fetch_level)
			{
				if (request_url.level_ptr[1] < REQUEST_URL_MAX_DEF) //is null
				{
				}
				else
				{
					int len = *request_url.data_ptr._int_ptr++;
					request_url.data_ptr._int_ptr++;
					url_out = request_url.data_ptr._byte_ptr;

					if (check(url_out, len))
					{
						result[result_count++] = url_out;
					}

					len++;
					len += len % 4;
					request_url.data_ptr._byte_ptr += len;

				}
				request_url.level_ptr += 2;
			}
		}
		if ((!columnar_eof(request_url)) && (next_level < request_url.level_ptr[0])) next_level = request_url.level_ptr[0];

		/* check end condition */
		if (!has_more_slice) break;

		fetch_level = next_level; //update fetch_level
		select_level = fetch_level; //update select level;
	}
	end_time = clock();
	printf("Result count %d\n", result_count);
	FILE* f = fopen(outfile, "wt");
	if (f)
	{
		for (int i = 0; i < result_count; i++)
		{
			fprintf(f, "q12result{\n\tRequests.URL: %s\n}\n", result[i]);
		}
	}
	else
	{
		printf("Can not open file for write result");
	}
	fclose(f);
	columnar_close(&request_url);
	printf("Speed:%fMB/sec\n", ((double) datasize / 1024 / 1024) * 1000.0 / ((double) (end_time - start_time) / (CLOCKS_PER_SEC / 1000)));
}

#endif /* Q12_H_ */
