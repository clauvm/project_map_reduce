from mrjob.job import MRJob
from mrjob.step import MRStep

m_rows = 0
m_columns = 0
n_rows = 0
n_columns = 0


class MRScaler_Product(MRJob):

    def map_values(self,file,_):
        global m_rows,m_columns,m_rows,n_columns
       
        x = open(file,"r")
        for line in x:
            row_lines = line.split()
            y= len(row_lines)
        
        if "1.txt" in file:
                while m_columns == 0:
                    m_columns = y-1
                    m_rows =m_rows+1
                 
        if "2.txt" in file:
                while n_columns == 0:
                     n_columns = y - 1
                     n_rows = n_rows+1
                 
        for k in range(y):
                     yield k, ("TEMP_1", m_rows,row_lines[k])
                     yield n_rows, ("TEMP_2", k, row_lines[k])

    
    def reducer_part1(self, key, values):
        M_listA =[]
        N_listB = []
        for val in values:
            if val[0] == "TEMP_1":
                M_listA.append(val)
            elif val[0] == "TEMP_2":
                N_listB.append(val)
        for m in M_listA:
            for n in N_listB:
                yield (m[1], n[1]),float(m[1]) * float(n[2])

      
    
    def mapper_part2(self, key, values):
        yield key, values      #mutiplied values are added

    def reducer_part2(self, key, values):
       
        yield (key, sum(values))
    
    
    def steps(self):
        return [MRStep(mapper_raw=self.map_values,      
                       reducer=self.reducer_part1),        
                MRStep(mapper=self.mapper_part2,          
                       reducer=self.reducer_part2)]     



if __name__ == '__main__':
    MRScaler_Product.run()
    