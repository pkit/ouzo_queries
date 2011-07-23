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
 * Implement for query Q4:
 * **************************************************************
 * 		SELECT
 * 			country,
 * 			COUNT(SessionID),
 * 			AVG(COUNT(Requests.requestID) WITHIN Record
 * 		FROM
 * 			Document
 * 		GROUP BY
 * 			country
 * **************************************************************
 */

#ifndef Q4_H_
#define Q4_H_
#include <google/dense_hash_set>
#include "columnar.h"

typedef struct
{
	char* country;
	int32_t count_session_id;
	int32_t avg_count_request_id;
} q4_slice;

struct q4_slice_equal
{
	inline bool operator()(const q4_slice *s1, const q4_slice *s2) const
	{
		return (s1 == s2) || (s1 && s2 && str_compare(s1->country, s2->country));
	}
};

struct q4_slice_hash
{
	inline size_t operator()(const q4_slice* s) const
	{
		return str_hash(s->country);
	}
};

typedef google::dense_hash_set<q4_slice*, q4_slice_hash, q4_slice_equal> q4_slice_set;

extern void* output_pool;
void q4(char* path, char* outfile)
{
	char tmp_path[1024];
	COMMON_VARIABLES_DECLARE;

	/* output */
	char* country_out = NULL;
	int64_t session_id_out;
	int64_t count_request_id_out = 0;

	sprintf(tmp_path, "%s/%s", path, "Document.country");
	COLUMNAR_DECLARE(country, tmp_path);
	sprintf(tmp_path, "%s/%s", path, "Document.SessionID");
	COLUMNAR_DECLARE(session_id, tmp_path);
	sprintf(tmp_path, "%s/%s", path, "Document.Requests.RequestID");
	COLUMNAR_COUNT_DECLARE(request_id, tmp_path);

	q4_slice_set dmap;
	dmap.set_empty_key(NULL);

	assert(output_pool);
	q4_slice* result = (q4_slice*) output_pool;
	int result_count = 0;

	start_time = clock();
	while (1)
	{
		next_level = 0;
		has_more_slice = 0;
		/* fetch country */
		if (!columnar_eof(country))
		{
			has_more_slice = 1;
			assert(country.level_ptr < country.level_end_buf);
			if (country.level_ptr[0] >= fetch_level)
			{
				if (country.level_ptr[1] < COUNTRY_MAX_DEF) //is null
				{
					country_out = "NULL";
				}
				else
				{
					assert(country.data_ptr._byte_ptr < country.data_buf+country.data_size);
					int len = *country.data_ptr._int_ptr++;
					country.data_ptr._int_ptr++;
					assert(country.data_ptr._byte_ptr < country.data_buf+country.data_size);
					country_out = country.data_ptr._byte_ptr;
					len++;
					int padding_count = len % 4;
					country.data_ptr._byte_ptr += len;
					country.data_ptr._byte_ptr += padding_count;
					assert(country.data_ptr._byte_ptr <= country.data_buf+country.data_size);
				}
				country.level_ptr += 2;
			}
		}
		if ((!columnar_eof(country)) && (next_level < country.level_ptr[0])) next_level = country.level_ptr[0];

		/* fetch session_id */

		if (!columnar_eof(session_id))
		{
			has_more_slice = 1;
			assert(session_id.level_ptr < session_id.level_end_buf);
			if (session_id.level_ptr[0] >= fetch_level)
			{
				if (session_id.level_ptr[1] < SESSION_ID_MAX_DEF) //is null (OPTIMIZATION HINT: not necessary because session_id is not null)
				{
					//session_id.value._long_ptr = (int64_t*) &LONG_NULL;
				}
				else
				{
					assert(session_id.data_ptr._byte_ptr < session_id.data_buf+session_id.data_size);
					session_id_out = *session_id.data_ptr._long_ptr++;
				}
				session_id.level_ptr += 2;
			}
		}
		if ((!columnar_eof(session_id)) && (next_level < session_id.level_ptr[0])) next_level = session_id.level_ptr[0];

		/* fetch request_id */
		if (!columnar_eof(request_id))
		{
			has_more_slice = 1;
			assert(request_id.level_ptr < request_id.level_end_buf);
			if (request_id.level_ptr[0] >= fetch_level)
			{
				if (request_id.level_ptr[1] < REQUEST_ID_MAX_DEF) //is null
				{
				}
				else
				{
					count_request_id_out++;
				}
				request_id.level_ptr += 2;
			}
		}
		if ((!columnar_eof(request_id)) && (next_level < request_id.level_ptr[0])) next_level = request_id.level_ptr[0];

		/* check end condition */
		if (!has_more_slice) break;

		fetch_level = next_level; //update fetch_level
		select_level = fetch_level; //update select level;

		if (select_level == 0) //WITHIN record
		{
			q4_slice* s = result + result_count;
			s->country = country_out;

			q4_slice_set::const_iterator it = dmap.find(s);
			if (it != dmap.end())
			{
				q4_slice* s = *it;
				s->avg_count_request_id += count_request_id_out;
				s->count_session_id++;
			}
			else
			{
				result_count++;
				s->country = country_out;
				s->count_session_id = 1;
				s->avg_count_request_id = count_request_id_out;
				dmap.insert(s);
			}
			count_request_id_out = 0;
		}
	}
	end_time = clock();

	printf("Result count %d\n", result_count);
	FILE* f = fopen(outfile, "wt");
	if (f)
	{
		for (int i = 0; i < result_count; i++)
		{
			fprintf(f, "q4result{\n\tcountry: %s\n\tCOUNT(SessionID): %d\n\tAVG(COUNT(Requests.requestID): %d\n}\n", result[i].country, result[i].count_session_id, result[i].avg_count_request_id);
		}
	}
	else
	{
		printf("Can not open file for write result");
	}
	fclose(f);

	columnar_close(&country);
	columnar_close(&session_id);
	columnar_close(&request_id);

	printf("Speed:%fMB/sec\n", ((double) datasize / 1024 / 1024) * 1000.0 / ((double) (end_time - start_time) / (CLOCKS_PER_SEC / 1000)));
}

//void q4_sqlite()
//{
//	Database db("../dremel/document.sqlite");
//	Query q(db);
//
//	bool r = q.get_result("select country, count(SessionID), sum(c_reqid) from (select Document.country as country, Document.SessionID as SessionID, req.c_reqid as c_reqid from Document left join (select docid, count(RequestID) as c_reqid from Requests group by docid) req on document.docid=req.docid) group by country order by country");
//	//	bool r = q.get_result("select docid, count(RequestID) as c_reqid from Requests group by docid");
//	if (r)
//	{
//		while (q.fetch_row())
//		{
//			const char* c = q.getstr();
//			long csid = q.getval();
//			long reqid = q.getval();
//			printf("%ld\t%ld\t%s\n", csid, reqid, c);
//		}
//	}
//	else printf("Not OK\n");
//
//	q.free_result();
//
//}

#endif /* Q4_H_ */
