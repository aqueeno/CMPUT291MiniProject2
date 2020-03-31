

.PHONY: build_index parse_files sort_files format_files
sort_files: scores.txt pterms.txt rterms.txt
	sort -u $^

parse_files: phase1.py 
	python3 phase1.py
	
rw.idx: rw.txt
	db_load -c duplicates=1 -T -f $< -t hash $@

pt.idx: pt.txt
	db_load -c duplicates=1 -T -f $< -t btree $@
	
sc.idx: sc.txt
	db_load -c duplicates=1 -T -f $< -t btree $@
	
rt.idx: rt.txt
	db_load -c duplicates=1 -T -f $< -t btree $@


format_files:  sort_files parse_files 

build_index: rw.idx pt.idx rt.idx sc.idx 


	
	

