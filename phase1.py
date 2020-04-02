def parse_file(orig_files, new_file):
    
    og_file = open(orig_files, "r")
    write_file = open(new_file, "a")
    
    lines = og_file.readlines()
    og_file.close()
    
    for i in range(len(lines)):
        lines[i] = lines[i].strip()
        lines[i] = lines[i].replace("\\", " ")
        lines[i] = lines[i].split(",", 1)
        
        write_file.write(lines[i][0])
        write_file.write("\n")
        write_file.write(lines[i][1])
        write_file.write("\n")
        
    write_file.close()
        
    

def main():
    files = ["pterms.txt", "reviews.txt", "rterms.txt", "scores.txt"]
    new_files = ["pt.txt", "rw.txt", "rt.txt", "sc.txt"]
    
    for i in range(len(files)):
        parse_file(files[i], new_files[i])
   
main()
