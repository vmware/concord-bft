import sys

print (sys.argv[1]) 
# print (sys.argv[2] )

filter_list1 = []
filter_list2 = []

class LineChecker():

    def __init__(self, filter_list):
        self.filter_list = filter_list

    def check(self, line):
        for f in self.filter_list:
            if f in line:             
                return True
        return False

with open(sys.argv[1]) as f:
    filter_list1 = f.readlines()


for filter in filter_list1:
    filter_list2.append(filter.strip())
file_lines = []
with open(sys.argv[2]) as f:
    file_lines = f.readlines()

filter_out = False
in_memory_leaks = False

lineChecker = LineChecker(filter_list2) 
section_to_print = []
for line in file_lines:
    line = line.rstrip()
    if in_memory_leaks:
        if 'leaked' in line:
            if not filter_out:
                for l in section_to_print:
                    print(l)
                print("")
                section_to_print.clear()
            filter_out = lineChecker.check(line)
            if not filter_out:
                section_to_print.append(line)
        elif not filter_out:
            filter_out = lineChecker.check(line)
            
            if not filter_out:
                section_to_print.append(line.rstrip())
            else: 
                section_to_print.clear()

    elif "MEMORY LEAKS" in line:
        in_memory_leaks = True 


