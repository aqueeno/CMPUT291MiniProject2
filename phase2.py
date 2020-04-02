from bsddb3 import db

from datetime import datetime
import shlex
# really helpful in parsing complicated text
# https://docs.python.org/3/library/shlex.html#parsing-rules
# may need to cite this

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
            # continue_query() #persistant < -- i don't think it needs to be recurisve. the while loop accomplises persistance

def process_text(string):
    
    all_queries = []
    operators = ["=", ":", "<", ">"]
    for operator in operators:

        while operator in string:
            left = string.split(operator, 1)[0]
            right = string.split(operator, 1)[1]
            
            left_operand = left.split()[-1]
            right_operand = right.split()[0]
            
            left = left.split()
            left.pop()
            right = right.split()
            right.pop(0)
            stri = " "
            string = stri.join(left+right)
            all_queries.append(left_operand + operator+right_operand)
    # add the inidivual standalone words to the query list
    string = string.split()
    for query in string:
        all_queries.append(query)
        
    return all_queries

def pterm_search(term, results):
    rev_ids = []
    if term.endswith("%"):
        term = term.replace("%", "")

        row = pt_cur.first()
        while row is not None:
            if row[0].decode("utf-8").startswith(term):
                rev_ids.append(row[1])

            row = pt_cur.next()
    else:

        row = pt_cur.set(term.encode("utf-8"))
        while row is not None:
            rev_ids.append(row[1])
            row = pt_cur.next_dup()

    results.append(set(rev_ids))

def rterm_search(term, results):
    rev_ids = []
    if term.endswith("%"):
        term = term.replace("%", "")

        row = rt_cur.first()
        while row is not None:
            if row[0].decode("utf-8").startswith(term):
                rev_ids.append(row[1])
            row = rt_cur.next()

    else:
        row = rt_cur.set(term.encode("utf-8"))
        while row is not None:
                rev_ids.append(row[1])
                row = rt_cur.next_dup()

    results.append(set(rev_ids))


def term_search(query, results):
    left = query.split(":")[0]
    term = query.split(":")[1]

    if left == "pterm":
        pterm_search(term, results)

    elif left == "rterm":
        rterm_search(term, results)

    #may need to handle errors?


def single_term_search(query, results):
    #contains term in at least one of product title, review summary, or review text in reviews.txt
    rev_ids = []
    if query.endswith("%"): #partial matching case 
        query = query.replace("%","")
        #first check product titles 
        pt_row = pt_cur.first()
        while pt_row is not None:
            if pt_row[0].decode("utf-8").startswith(query):
                rev_ids.append(pt_row[1])
            pt_row = pt_cur.next()
        #then check review summaries/texts
        rt_row = rt_cur.first()
        while rt_row is not None:
            if rt_row[0].decode("utf-8").startswith(query):
                rev_ids.append(rt_row[1])
            rt_row = rt_cur.next()
    else: #case for single, exact term
        #first check product titles
        pt_row = pt_cur.set(query.encode("utf-8"))
        while pt_row is not None:
            rev_ids.append(pt_row[1])
            pt_row = pt_cur.next_dup()
        #then check review summaries/texts
        rt_row = rt_cur.set(query.encode("utf-8"))
        while rt_row is not None:
            rev_ids.append(rt_row[1])
            rt_row = rt_cur.next_dup()

    results.append(set(rev_ids))

def range_search_bigger(query, results):
    query_bigger_operation = query.split(">")
    rev_ids = []
    row = rw_cur.first()
    while row is not None:
        if query_bigger_operation[0] == "price":
            product_price = row[3].decode("utf-8")
            if query_bigger_operation[1] > int(product_price):
                rev_ids.append(row[0])
            row = rt_cur.next()
        elif query_bigger_operation[0] == "score":
            product_score = row[7].decode("utf-8")
            if query_bigger_operation[1] > int(product_score):
                rev_ids.append(row[0])
            row = rt_cur.next()
        elif query_bigger_operation[0] == "date":
            product_date = row[8].decode("utf-8")
            query_timestamp = int(datetime.strptime(query_bigger_operation[1], '%Y/%m/%d').timetuple())
            if  query_timestamp > int(product_date):
                rev_ids.append(row[0])
            row = rt_cur.next()
    results.append(set(rev_ids))

def range_search_smaller(query, results):
    query_bigger_operation = query.split("<")
    rev_ids = []
    row = rw_cur.first()
    while row is not None:
        if query_bigger_operation[0] == "price":
            product_price = row[3].decode("utf-8")
            if query_bigger_operation[1] < int(product_price):
                rev_ids.append(row[0])
            row = rt_cur.next()
        elif query_bigger_operation[0] == "score":
            product_score = row[7].decode("utf-8")
            if query_bigger_operation[1] < int(product_score):
                rev_ids.append(row[0])
            row = rt_cur.next()
        elif query_bigger_operation[0] == "date":
            product_date = row[8].decode("utf-8")
            query_timestamp = int(datetime.strptime(query_bigger_operation[1], '%Y/%m/%d').timetuple())
            if  query_timestamp < int(product_date):
                rev_ids.append(row[0])
            row = rt_cur.next()
    results.append(set(rev_ids))


def compute_results(queries, results):
    global OUTPUT
    #note print statements are just placeholders. Need functions to go here
    # in each function, handle partial matching (% symbol)
    for query in queries:
        if "<" in query:
            range_search_smaller(query, results)
            #lessThan function

        elif ">" in query:
            range_search_bigger(query, results)
            #greaterThan function

        elif ":" in query:
            term_search(query, results)

        elif "output=" in query:
            OUTPUT = query.split("=")[1]
            #need to handle errors!!!

        else: #single words 
            single_term_search(query, results)




def intersect(id_set):
    
    intersect_ids = None
    
    for i in range(len(id_set)):
        intersect_ids = id_set[0].intersection(id_set[i])
    
    return intersect_ids

    #extra space deletion
    '''
    string = str(string)

    string = " ".join(string.split())
    string = string.split()
    operations = []
    operators = ["=", ":", "<", ">"]
    used_operators = []
    for operator in operators:
        for i in range(len(string)-1):
            if string[i] == operator and i>0:
                operations.append(string[i-1] + string[i] + string[i+1])
                used_operators.append(i-1)
                used_operators.append(i)
                used_operators.append(i+1)
            else:
                #in this case it will be illegal element
                continue
    for i in range(len(string)):
        if i not in used_operators:
            operations.append(string[i])
    print(operations)
    '''

def print_table(results):
    
    if not results:
        print("\nNo results matching the query...\n")
        return
    results = list(results)
    
    if OUTPUT == "brief":
        fields = ["Product Title", "Review Score"]
        settings = [1, 6]
    elif OUTPUT == "full":
        fields = ["Product ID" ,"Product Title", "Price", 
                  "User ID","User Name", "Helpfulness","Review Score","Timestamp", "Summary", "Full Review"]
        settings = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    print("")
    for ID in results:
        string = rw_db.get(ID).decode("utf-8")
        
        # this code seperates based on commas but keeps strings in 
        # quotes together.
        # Ex. if string = '1, 2, "3, 4"', it would be split as ['1', '2', '3, 4']
        row = shlex.shlex(string, punctuation_chars=True)
        row.whitespace_split = False
        row = list(row)
        while "," in row:
            row.remove(",")
        
        print("Review ID:", ID.decode("utf-8"))
        
        for idx in range(len(fields)):
            print(fields[idx]+":", row[settings[idx]])
        print("")

def main():
    init_databases()
    init_cursors()


    while True:
        results = [] #all review_ids are added here
        user_input = input("Enter your search:\n> ")

        # queries is a list containing ALL queries
        # if the user entered "guitar price > 50"
        # this list would contain ["guitar", "price>50"]
        queries = process_text(user_input.lower())
        # compute each query and add the searches to "results"
        compute_results(queries, results)
        
        #for i in results: #for testing purposes. Delete later
        #    print(i)

        ids = intersect(results) #get intersection of the tuple sets in this function
        #print(ids)
        print_table(ids)

        if continue_query():
            break

    print("\nGoodbye\n")
    close_connection()
def test():
    string = '1,2,"hello,world",7/7,3'
    
    s = shlex.shlex(string, punctuation_chars = True)#posix=True)
    s.whitespace_split = False
    s = list(s)
    while "," in s:
        s.remove(",")
    
    print(s)

    

#def main():
 #   intersect("guitar date>                  2007/05/16  price    >    200 price   < 300")
  #  return
if __name__ == "__main__":
    
    main()
    #test()
 #   main()
    #print(process_text(" score   :  4   guitar%   price   >   50  "))# was testing if I can seperate all the queries
