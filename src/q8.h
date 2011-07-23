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
 * SELECT
 * 		country,
 * 		agent,
 * 		requestCount
 * FROM
 * 		(SELECT
 * 			country,
 * 			agent,
 * 			SUM(COUNT(Request.RequestID) WITHIN Record) as requestCount
 * 		FROM
 * 			Documents GROUP BY country,
 * 			agent)
 * WHERE requestCount>100
 */

#ifndef Q8_H_
#define Q8_H_
#include <google/dense_hash_set>
#include "columnar.h"

struct q8_slice
{
	char* country;
	char* agent;
	int request_count;
};

struct q8_slice_equal
{
	inline bool operator()(const q8_slice *s1, const q8_slice *s2) const
	{
		//return (s1 == s2) || (s1 && s2 && strcmp(s1->country, s2->country) == 0 && strcmp(s1->agent, s2->agent) == 0);
		return (s1 == s2) || (s1 && s2 && str_compare(s1->country, s2->country) && str_compare(s1->agent, s2->agent));
	}
};

struct q8_slice_hash
{
	inline size_t operator()(const q8_slice* s) const
	{
		size_t h1 = str_hash(s->country);
		size_t h2 = str_hash(s->agent);
		return h1 ^ h2;
	}
};

typedef google::dense_hash_set<q8_slice*, q8_slice_hash, q8_slice_equal> q8_slice_set;
#define process_null_for_country(val)		{country_out=val;}
#define process_value_for_country(val)		{country_out=val;}
#define process_null_for_agent(val)			{agent_out=val;}
#define process_value_for_agent(val)		{agent_out=val;}
#define process_count_for_request_id(val)	{request_count++;}

extern void* output_pool;
void q8(char* path, char* outfile)
{
	char tmp_path[1024];
	COMMON_VARIABLES_DECLARE;
	sprintf(tmp_path,"%s/%s",path, "Document.country");
	COLUMNAR_DECLARE(country,tmp_path)
	sprintf(tmp_path,"%s/%s",path, "Document.Agent");
	COLUMNAR_DECLARE(agent, tmp_path);
	sprintf(tmp_path,"%s/%s",path, "Document.Requests.RequestID");
	COLUMNAR_COUNT_DECLARE(request_id, tmp_path);

	char* country_out = STRING_NULL;
	char* agent_out = STRING_NULL;
	int request_count = 0;

	q8_slice_set resultset;
	resultset.set_empty_key(NULL);
	q8_slice* result = (q8_slice*) output_pool;
	int result_count = 0;

	start_time = clock();
	while (1)
	{
		next_level = 0;
		has_more_slice =0;
		fetch_str(country, COUNTRY_MAX_DEF);
		fetch_str(agent, AGENT_MAX_DEF);
		fetch_count_only(request_id, REQUEST_ID_MAX_DEF);

		/* check end condition */
		if (!has_more_slice) break;

		fetch_level = next_level; //update fetch_level
		select_level = fetch_level; //update select level;

		if (select_level == 0) //WITHIN record
		{
			q8_slice* s = result + result_count;
			s->agent = agent_out;
			s->country = country_out;

			q8_slice_set::const_iterator it = resultset.find(s);
			if (it == resultset.end())
			{
				s->request_count = request_count;
				resultset.insert(s);
				result_count++;
			}
			else
			{
				(*it)->request_count += request_count;
			}
			request_count = 0;
		}
	}
	end_time = clock();

	FILE* f = fopen(outfile, "wt");
	if (f)
	{
		for (q8_slice_set::const_iterator it = resultset.begin(); it != resultset.end(); ++it)
		{
			if ((*it)->request_count > 0)
			{
				//printf("%d\t%20s\t%s\n", (*it)->request_count, (*it)->country, (*it)->agent);
				fprintf(f, "q8result{\n\tcountry: %s\n\tagent: %s\n\tRequestCount: %d\n}\n", (*it)->country, (*it)->agent, (*it)->request_count);
			}
		}
	}
	else
	{
		printf("Can not open file for write result");
	}
	fclose(f);

	columnar_close(&country);
	columnar_close(&agent);
	columnar_close(&request_id);

	printf("Speed:%fMB/sec\n", ((double) datasize / 1024 / 1024) * 1000.0 / ((double) (end_time - start_time) / (CLOCKS_PER_SEC / 1000)));
}


#undef process_null_for_country
#undef process_value_for_country
#undef process_null_for_agent
#undef process_value_for_agent
#undef process_count_for_request_id

#endif /* Q8_H_ */
