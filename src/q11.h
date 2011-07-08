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
 * Implement for query Q11:
 * *************************************************************************
 * SELECT
 * 		TOP 10 (Requests.URL),
 * 		COUNT (Requests.URL)
 * FROM
 * 		Documents
 * *************************************************************************
 */

#ifndef Q11_H_
#define Q11_H_

#include <google/dense_hash_map>
#include "columnar.h"
#include <vector>

struct q11_slice
{
	int count;
	char* url;
};

struct q11_str_equal
{
	inline bool operator()(const char *s1, const char *s2) const
	{
		return (s1 == s2) || (str_compare(s1, s2));
	}
};

struct q11_str_hash
{
	inline size_t operator()(const char* s) const
	{
		return str_hash(s);
	}
};

typedef google::dense_hash_map<const char*, q11_slice*, q11_str_hash, q11_str_equal> strhash;

bool sort_by_count(const q11_slice* li, const q11_slice* ri)
{
	return li->count < ri->count;
}
#define process_null_for_request_url(val)	/*nothing*/

#define process_value_for_request_url(val)\
{\
	url_out = val;\
	strhash::const_iterator it = dmap.find(url_out);\
	if (it == dmap.end())\
	{\
		q11_slice* r = result_set+result_count;\
		result_count++;\
		r->count = 1;\
		r->url = url_out;\
		result.push_back(r);\
		dmap[url_out] = r;\
	}\
	else\
	{\
		it->second->count++;\
	}\
}

extern void* output_pool;
void q11(char* path, char* outfile)
{
	char tmp_path[1024];
	q11_slice* result_set = (q11_slice*) output_pool;
	int result_count = 0;

	std::vector<q11_slice*> result;
	COMMON_VARIABLES_DECLARE;
	sprintf(tmp_path,"%s/%s",path, "Document.Requests.URL");
	COLUMNAR_DECLARE(request_url, tmp_path);
	char* url_out;

	strhash dmap;
	dmap.set_empty_key("");

	start_time = clock();
	while (has_more_slice)
	{
		next_level = 0;
		fetch_str(request_url, REQUEST_URL_MAX_DEF);

		/* check end condition */
		if (columnar_eof(request_url)) has_more_slice = 0;
		fetch_level = next_level; //update fetch_level
		select_level = fetch_level; //update select level;
	}
	std::nth_element(result.begin(), result.begin() + 10, result.end(), sort_by_count);
	end_time = clock();

	FILE* f = fopen(outfile, "wt");
	if (f)
	{
		for (int j = 0; j < 10; j++)
		{
			q11_slice* r = result.at(j);
			//printf("\t%d\t%s\n", r->count, r->url);
			fprintf(f, "q11result{\n\tRequests.URL: %s\n\tCOUNT(Requests.URL): %d\n}\n", r->url,r->count);
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

#endif /* Q11_H_ */
