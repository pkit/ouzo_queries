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
 * Implement for query Q5:
 * ***************************************************************************************************
 * SELECT
 * 		COUNT(DISTINCT(Requests.URL))
 * 	FROM
 * 		Documents
 * 	WHERE
 * 		Requests.RequestDate > '01-01-2010' AND Requests.RequestDate < '01-01-2011'
 * ***************************************************************************************************
 */

#ifndef Q5_H_
#define Q5_H_
#include <google/dense_hash_map>
#include "columnar.h"

void q5(char* path, char* outfile)
{
	char tmp_path[1024];
	COMMON_VARIABLES_DECLARE;
	sprintf(tmp_path, "%s/%s", path, "Document.Requests.RequestTime");
	COLUMNAR_DECLARE(request_time, tmp_path);
	sprintf(tmp_path, "%s/%s", path, "Document.Requests.URL");
	COLUMNAR_DECLARE(request_url, tmp_path);

	int trailSize = 0;
	unsigned int max_table_size = 450;
	int64_t time_out;
	char* url_out;

	struct tm tm1;
	struct tm tm2;
	strptime("2010-1-1 00:00:00", "%Y-%m-%d %H:%M:%S", &tm1);
	strptime("2011-1-1 00:00:00", "%Y-%m-%d %H:%M:%S", &tm2);
	time_t t1 = mktime(&tm1);
	time_t t2 = mktime(&tm2);

	google::dense_hash_map<unsigned int, int> dmap;
	dmap.set_empty_key(-1);
	dmap.set_deleted_key(-2);

	start_time = clock();
	while (has_more_slice)
	{
		next_level = 0;
		has_more_slice = 0;
		/* fetch country */
		if (!columnar_eof(request_time))
		{
			has_more_slice = 1;
			assert(request_time.level_ptr<request_time.level_end_buf);
			if (request_time.level_ptr[0] >= fetch_level)
			{
				if (request_time.level_ptr[1] < REQUEST_TIME_MAX_DEF) //is null
				{
				}
				else
				{
					assert(request_time.data_ptr._byte_ptr < request_time.data_buf + request_time.data_size);
					time_out = *request_time.data_ptr._long_ptr++;
				}
				request_time.level_ptr += 2;
			}
		}
		if ((!columnar_eof(request_time)) && (next_level < request_time.level_ptr[0])) next_level = request_time.level_ptr[0];

		/* fetch request_id */
		if (!columnar_eof(request_url))
		{
			has_more_slice =1;
			assert(request_url.level_ptr < request_url.level_end_buf);
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

					len++;
					len += len % 4;
					request_url.data_ptr._byte_ptr += len;

					if (time_out > t1 && time_out < t2)
					{
						unsigned int hash_code = str_hash(url_out);
						int trail = __builtin_ctz(hash_code);

						if (trail >= trailSize)
						{
							dmap[hash_code] = trail;

							if (dmap.size() > max_table_size)
							{
								trailSize++;
								for (google::dense_hash_map<unsigned int, int>::const_iterator it = dmap.begin(); it != dmap.end(); ++it)
								{
									if (it->second < trailSize) dmap.erase(it->first);
								}
							}
						}
					}
				}
				request_url.level_ptr += 2;
			}
		}
		if (next_level < request_url.level_ptr[0]) next_level = request_url.level_ptr[0];

		fetch_level = next_level; //update fetch_level
		select_level = fetch_level; //update select level;
	}
	end_time = clock();

	FILE* f = fopen(outfile, "wt");
	if (f)
	{
		//printf("Estimate size:%d\n", dmap.size() * (1 << trailSize));
		fprintf(f, "q12result{\n\tCOUNT(DISTINCT(Requests.URL)): %d\n}\n", dmap.size() * (1 << trailSize));
	}
	else
	{
		printf("Can not open file for write result");
	}
	fclose(f);

	columnar_close(&request_time);
	columnar_close(&request_url);
	printf("Speed:%fMB/sec\n", ((double) datasize / 1024 / 1024) * 1000.0 / ((double) (end_time - start_time) / (CLOCKS_PER_SEC / 1000)));
}

#endif /* Q5_H_ */
