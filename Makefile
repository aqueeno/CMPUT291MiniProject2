

.PHONY: build_index parse_files sort_files
sort_files: scores.txt pterms.txt rterms.txt
	sort -u $^

parse_files: phase1.py 
	python3 phase1.py
	
rw.idx: parsed_reviews.txt
	db_load -T -f $< -t hash $@

pt.idx: parsed_pterms.txt
	db_load -T -f $< -t btree $@
	
sc.idx: parsed_scores.txt
	db_load -T -f $< -t btree $@
	
rt.idx: parsed_rterms.txt
	db_load -T -f $< -t btree $@


build_index:  sort_files parse_files rw.idx pt.idx rt.idx sc.idx 


	
	

