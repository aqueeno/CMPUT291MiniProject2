from bsddb3 import db 

rw_db = None
rw_cur = None
pt_db = None
pt_cur = None
rt_db = None
rt_cur = None
sc_db = None
sc_cur = None 

OUTPUT = "brief"

def init_databases():
    global rw_db, pt_db, rt_db, sc_db
    
    rw_db = db.DB()
    pt_db = db.DB()
    rt_db = db.DB()
    sc_db = db.DB()
    
    pt_db.set_flags(db.DB_DUP)
    rt_db.set_flags(db.DB_DUP)
    sc_db.set_flags(db.DB_DUP)
    
    rw_db.open("rw.idx")
    pt_db.open("pt.idx")
    rt_db.open("rt.idx")
    sc_db.open("sc.idx")
    
    
def init_cursors():
    global rw_cur, pt_cur, rt_cur, sc_cur
    rw_cur = rw_db.cursor()
    pt_cur = pt_db.cursor()
    rt_cur = rt_db.cursor()
    sc_cur = sc_db.cursor()
    
def close_connection():
    global rw_db, pt_db, rt_db, sc_db
    global rw_cur, pt_cur, rt_cur, sc_cur
    
    rw_cur.close() 
    pt_cur.close() 
    rt_cur.close() 
    sc_cur.close() 
    
    rw_db.close()
    pt_db.close()
    rt_db.close()
    sc_db.close()
    
def continue_query():
    while True:
        cont = input("Would you like to search again? (y/n)\n> ")
        if cont.lower().startswith("n"):
            return True
        elif cont.lower().startswith("y"):
            return False
        else:
            print("Invalid input.\n")
    

def process_text(string):
    all_queries = []
    operators = ["=", ":", "<", ">"]
    for operator in operators:
        
        while operator in string:
            left = string.split(operator)[0]
            right = string.split(operator)[1]
            left_operand = left.split()[-1]
            right_operand = right.split()[0]
            all_queries.append(left_operand + operator+right_operand)
            string = string.replace(operator, "", 1)
            string = string.replace(left_operand, "", 1)
            string = string.replace(right_operand, "", 1)
    
    string = string.split()
    for word in string:
        all_queries.append(word)
        
    return all_queries

def term_search(query, results):
    left = query.split(":")[0]
    term = query.split(":")[1].encode("utf-8")
    
    if left == "pterm":
        row = pt_cur.set(term)
        
        while row is not None:
            #res = rw_db.get(row[1])
            #results.append(res)
            results.append(row[1])
            row = pt_cur.next_dup()
            
    elif left == "rterm":
        row = rt_cur.set(term)
        
        while row is not None:
            #res = rw_db.get(row[1])
            #results.append(res)
            results.append(row[1])
            row = rt_cur.next_dup()
    else:
        print("Invalid input!")
    
    
def compute_results(queries, results):
    #note print statements are just placeholders. Need functions to go here
    # in each function, handle partial matching (% symbol)
    for query in queries:
        if "<" in query:
            print("<")
            #some function
        elif ">" in query:
            print(">")
            #lessThan function
        elif ":" in query:
            print(":")
            term_search(query, results)
            
        elif "output=" in query:
            print("change output boys")
            
        else:
            print("handle single word")








def intersect(results):
    pass

def print_table(results):
    pass
    
    
    
    
    
def main():
    init_databases()
    init_cursors()
    
    
    while True:
        results = [] #all review_ids are added here would be added in here
        user_input = input("Enter your search:\n> ")
        
        # queries is a list containing ALL queries
        # if the user entered "guitar price > 50"
        # this list would contain ["guitar", "price>50"]
        queries = process_text(user_input.lower())
        # compute each query and add the searches to "results"
        compute_results(queries, results)
        #for i in results:
         #   print(i)
            
        intersect(results) #get intersection of the tuple sets in this function
        print_table(results)
        
        if continue_query():
            break
        
    print("\nGoodbye\n")
    close_connection()
    
if __name__ == "__main__":
    
    main()
    #print(process_text(" score   :  4   guitar%   price   >   50  "))# was testing if I can seperate all the queries
    
    
    
