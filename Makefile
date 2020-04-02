

.PHONY: build_index parse_files  format_files


scores: scores.txt
	sort -u -o $< $<
pterms: pterms.txt
	sort -u -o $< $<
rterms: rterms.txt
	sort -u -o $< $<
	

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


format_files: scores pterms rterms parse_files

build_index: rw.idx pt.idx rt.idx sc.idx 


	
	

