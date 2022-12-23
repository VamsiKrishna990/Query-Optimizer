import java.math.*;

class CostOfData
{
	public static int buffMemWB=50;
    public static int buffMemlB=30;
    public static int TupPage1;
    public static int TupPage2;
    public static int TupPage3;    
    public static final int pageSize=4096;
    public static final int blockSize=100;
    public static int total1;
    public static int total2;
    public static int total3;    
    static String output="";
    public static boolean isSortergeDone=false;
    public static int table1s=20;
    public static int table1p=1000;
    public static int table2s=40;
    public static int table2p=500;
    public static int table3s=100;
    public static int table3p=2000;
    Long costJoin =Long.valueOf(0);
    Boolean isinReverse;
    String JType;
}

public class QueryOptimizer extends CostOfData
{   
    public static String Q1t="Join t1 t2\n"+"Join temp1 t3\n"+"Project temp2\n"+"GroupBy temp3";
    public static String RQ1t="Join t1 t3\n"+"GroupBy temp0\n"+"Join t1 Temp1\n"+"Join t2 Temp2\n"+"Project temp3\n"+"GroupBy temp3";
    
    public QueryOptimizer()
    {
		output="";
	}
     
    public static void main(String[] args)
    {
        QueryProcessor(table1s, table1p, table2s, table2p, table3s, table3p);
    }

    public static void QueryProcessor(int table1s,int t1Pages,int table2s,int t2Pages,int table3s,int t3Pages)
    {   
        total1=(t1Pages*pageSize)/table1s;
        total2=(t2Pages*pageSize)/table2s;
        total3=(t3Pages*pageSize)/table3s;
        TupPage1=pageSize/table1s;
        TupPage2=pageSize/table2s;
        TupPage3=pageSize/table3s;     
        long cost1=ProcessOfQ1(t1Pages,t2Pages,t3Pages);
        long cost2=ProcessOfRQ1(t1Pages,t2Pages,t3Pages);        
        display(cost1, cost2);        
    }

    public static String queryProcessingCost(long IOCost)
    {
	    BigInteger ioCost=BigInteger.valueOf(IOCost).multiply(BigInteger.valueOf(12)).divide(BigInteger.valueOf(1000));
	    int totalSeconds=ioCost.intValue();
	    int seconds=totalSeconds % 60;
        int totalMinutes=totalSeconds / 60;
        int minutes=totalMinutes % 60;
        int hours=totalMinutes / 60;
        return hours + " hr(s) " + minutes + " min(s) " + seconds + " sec(s)";
    }

    public static long ProcessOfQ1(int t1Pages, int t2Pages, int t3Pages)
    {
        System.out.println("-----------------------------------------------------------\n");
        System.out.println("Cost of Q1:");
        System.out.println("-----------------------------------------------------------\n");
        long totalamt = 0;
        int tempT = 0;
        for (String line : Q1t.split("\n"))
        {
            if (line.toLowerCase().contains("join"))
            {
                String[] elements = line.split(" ");
                String leftTable = elements[1];
                String rightTable = elements[2];
                if (leftTable.equals("t1") && rightTable.equals("t2"))
                {
                    if (t1Pages > t2Pages)
                    {
                        CostOfData select = new CostOfData();
                        MinimumJoinCost(t2Pages, t1Pages, select, 1);
                        tempT = TablesJoin(t1Pages, t2Pages);
                        tempT = (tempT * 15) / 100;
                        totalamt += select.costJoin;
                        if (select.isinReverse)
                        {
                            System.out.println(elements[0] + " " + elements[2] + " " + elements[1]);
                        }
                        else
                        {
                            System.out.println(elements[0] +" " + elements[1] + " " + elements[2]);
                        }
                        System.out.println("->Cost: " + select.costJoin + " -Type of Join:  " + select.JType + "\n");
                    }
                }
                else if (leftTable.equals("temp1"))
                {
                    CostOfData select2 = new CostOfData();
                    CostOfTNLJoin(t3Pages, tempT, TupPage3, select2);
                    tempT = (tempT * 10 * 20) / 10000;
                    totalamt += select2.costJoin;
                    if (select2.isinReverse)
                    {
                        System.out.println(elements[0] +" " + elements[2] + " " + elements[1]);
                    }
                    else
                    {
                        System.out.println(elements[0] +" " + elements[1] + " " + elements[2]);
                    }
                    System.out.println("->Cost: " + select2.costJoin + " -Type of Join: " + select2.JType + "\n");
                }
            }
            if (line.toLowerCase().contains("project"))
            {
                String[] elements = line.split(" ");
                System.out.println(elements[0] + " " + elements[1] + "->Cost is too small so we can neglect it\n");
            }
            if (line.toLowerCase().contains("groupby"))
            {
                String[] elements = line.split(" ");
                int groupByCost = CostofGroupBy(tempT, t3Pages, false);
                totalamt += groupByCost;
                System.out.println(elements[0] + " " + elements[1] + "->Cost: " + groupByCost + "\n");
            }
        }
        System.out.println("I/O Cost of the Disk is: " + totalamt + "\n");
        System.out.println("Cost of Query Processing: " + queryProcessingCost(totalamt) + "\n");
        return totalamt;
    }
    
    public static long ProcessOfRQ1(int t1Pages,int t2Pages,int t3Pages)
    {
        System.out.println("-----------------------------------------------------------\n");
        System.out.println("cost of RQ1:");
        System.out.println("-----------------------------------------------------------\n");
	    long totalCost=0;
	    int tempT=0;
        for (String line : RQ1t.split("\n"))
        {			
			if (line.toLowerCase().contains("join"))
            {
				String[] elements=line.split(" ");
				String leftTable=elements[1];
				String rightTable=elements[2];
				if(leftTable.equals("t1") && rightTable.equals("t3"))
                {
					if(t3Pages > t1Pages)
                    {
						CostOfData select=new CostOfData();
						MinimumJoinCost(t3Pages, t1Pages, select, 3);
						tempT=TablesJoin(t3Pages, t1Pages);
						tempT=(tempT*20)/100;
						totalCost+=select.costJoin;
						if(select.isinReverse)
                        {
                            System.out.println(elements[0] + " " + elements[2] + " " + elements[1]);
					    }
                        else
                        {
                            System.out.println(elements[0] + " " + elements[1] + " " + elements[2]);
					    }
                        System.out.println("->Cost: " + select.costJoin + " -Type of Join: " + select.JType + "\n");			 }
				    }
                    else if(leftTable.equals("t1"))
                    {
                        CostOfData select1=new CostOfData();
                        MinimumJoinCost(t1Pages, tempT, select1, 4);
                        tempT=(tempT*15)/100;
                        totalCost+=select1.costJoin;
                        if(select1.isinReverse)
                        {
                            System.out.println(elements[0] + " " + elements[2] + " " + elements[1]);
                        }
                        else
                        {
                            System.out.println(elements[0] + " " + elements[1] + " " + elements[2]);
                        }
                        System.out.println("->Cost: " + select1.costJoin + " -Type of Join: " + select1.JType+ "\n");			
                    }
                    else if(leftTable.equals("t2"))
                    {
					    CostOfData select2=new CostOfData();
					    MinimumJoinCost(t2Pages, tempT, select2, 5);
					    tempT=(tempT*10)/100;
					    totalCost+=select2.costJoin;
					    if(select2.isinReverse)
                        {
						    System.out.println(elements[0] + " " + elements[2] + " " + elements[1] + "->Cost: " + select2.costJoin + " -Type of Join: " + select2.JType + " " + "\n");
					    }else
                        {
						    System.out.println(elements[0] + " " + elements[1] + " " + elements[2] + "->Cost: " + select2.costJoin + " -Type of Join: " + select2.JType+"\n");
					    }
					}
            }
			if (line.toLowerCase().contains("project"))
            {
				String[] elements=line.split(" ");
				System.out.println(elements[0] + " " + elements[1] + "->Cost is too small so we can neglect it\n");
			}
			if (line.toLowerCase().contains("groupby"))
            {
				String[] elements=line.split(" ");
				int groupByCost=0;
				if(elements[1].equals("temp0"))
                {
					groupByCost=CostofGroupBy(t1Pages, t3Pages, isSortergeDone);
				} 
                else
                {
					groupByCost=CostofGroupBy(tempT, t3Pages, isSortergeDone);
				}
				totalCost+=groupByCost;
				System.out.println(elements[0] + " " + elements[1]+"->Cost: " + groupByCost + "\n");
			}			
        }
		System.out.println("I/O Cost of the Disk is: " + totalCost + "\n");
		System.out.println("Cost of Query Processing:" + queryProcessingCost(totalCost) + "\n");
	    return totalCost;
    }

    public static int TablesJoin(int table1, int table2)
    {
        return table1 * table2;
    }

    public static int CostofGroupBy(int table1, int table2, boolean Sorted)
    {
        if(Sorted)
        {
            return 2 * (table1 + table2);
        }
        else
        {
            return 3 * (table1 + table2);
        }
    }
    private static long MinimumJoinCost(int leftTable, int rightTable, CostOfData obj, int type)
    {	
        long cost=0;
        long costa=0;
        long costb=0;
        long costc=0;
        long costd=0;
        long coste=0;
        long costf=0;
        long TNLcost=0;
        long PNLcost=0;
        long Hjcost1=0;
        long SMJcost1=0;
        long BNLcost1=0;
        long Hjcost2=0;
        long SMJcost2=0;
        long BNLcost2=0;      
        if(type==1)
        {
	       TNLcost=CostOfTNLJoin(leftTable, rightTable, TupPage1, obj);
        }
        else if(type==2)
        {
           TNLcost=CostOfTNLJoin(leftTable, rightTable, TupPage3, obj);
        }
        else if(type==3)
        {    
            TNLcost=CostOfTNLJoin(leftTable, rightTable, TupPage1, obj);
        }
        else if(type==4)
        {    
            TNLcost=CostOfTNLJoin(leftTable, rightTable, TupPage1, obj);
        }
        else if(type==5)
        {    
            TNLcost=CostOfTNLJoin(leftTable, rightTable, TupPage2, obj);
        }
        PNLcost=CostOfPNLJoin(leftTable, rightTable, obj);
	    Hjcost1=CostOfhashJoinWithBuffer(leftTable, rightTable, obj);
        Hjcost2=CostOfhashJoinWithLessBuffer(leftTable, rightTable, obj);
	    SMJcost1=CostOfSMJJoinWithBuf(leftTable, rightTable, obj);
	    SMJcost2=CostOfSMJJoinWithLessBuf(leftTable, rightTable, obj);
        BNLcost1=CostOfBNLJoinWithBuf(leftTable, rightTable, obj);
        BNLcost2=CostOfBNLJoinWithLessBuf(leftTable, rightTable, obj);
        costa=Math.min(Hjcost1, Hjcost2);   
        costb=Math.min(SMJcost1, SMJcost2);
        costc=Math.min(BNLcost1, BNLcost2);
        costd=Math.min(TNLcost, costa);
        coste=Math.min(costb, costc);
        costf=Math.min(costd, coste);
        cost=Math.min(PNLcost,costf);
        return cost;    
    }

    public static long CostOfTNLJoin(int leftTable, int rightTable, int tuplesPerPage, CostOfData obj)
    {
        long cost = (leftTable + tuplesPerPage * leftTable * rightTable);
        if((cost>=0))
        {
            obj.costJoin=cost;
            obj.JType="TNL";
            obj.isinReverse=true;
        }
        return cost;
    }

    public static long CostOfPNLJoin(int leftTable, int rightTable, CostOfData obj)
    {
        long cost = leftTable + leftTable*rightTable;
        if(obj.costJoin>cost || ((obj.costJoin==0) && cost>0))
        { 
            obj.costJoin=cost;
            obj.JType="PNL";
        }
        return cost;
    }

    public static long CostOfBNLJoinWithBuf(int leftTable, int rightTable, CostOfData obj)
    {
        int bufferMemory=buffMemWB;
        long cost=(leftTable+(leftTable/bufferMemory)*rightTable);
        if(obj.costJoin>cost || ( (obj.costJoin==0) && cost>0 ))
        {
            obj.costJoin=cost;
            obj.JType="BNL buffer = 50";
        }
        return cost;
    }

    public static long CostOfBNLJoinWithLessBuf(int leftTable, int rightTable, CostOfData obj)
    {
        int bufferMemory = buffMemlB;
        long cost = (leftTable+(leftTable/bufferMemory)*rightTable);
        if(obj.costJoin>cost || ((obj.costJoin==0) && cost>0))
        {
            obj.costJoin=cost;
            obj.JType="BNL buffer = 30";
            isSortergeDone=true;
            obj.isinReverse=false;
        }
        return cost;
    }

    public static long CostOfSMJJoinWithBuf(int leftTable, int rightTable, CostOfData obj)
    {
        long cost;	
        int bufferMemory = buffMemWB;
        double mMultiplier = (Math.log10(leftTable/bufferMemory))/(Math.log10(bufferMemory-1));
        double nMultiplier = (Math.log10(rightTable/bufferMemory))/(Math.log10(bufferMemory-1));
        cost = (int)Math.ceil(2*leftTable*(1+mMultiplier)+2*rightTable*(1+nMultiplier)+leftTable+rightTable);
        if(obj.costJoin>cost || ((obj.costJoin==0) && cost>0))
        {
            obj.costJoin=cost;
            obj.JType="SMJ buffer = 50";
            obj.isinReverse=false;
            isSortergeDone=true;
        }
        return cost;
    }

    public static long CostOfSMJJoinWithLessBuf(int leftTable, int rightTable, CostOfData obj)
    {
        long cost;	
		int bufferMemory = buffMemlB;
		double mMultiplier = Math.log10(leftTable/bufferMemory)/Math.log10(bufferMemory-1);
		double nMultiplier = Math.log10(rightTable/bufferMemory)/Math.log10(bufferMemory-1);
        cost = (int)Math.ceil((2*leftTable*(1+mMultiplier)+2*rightTable*(1+nMultiplier)+leftTable+rightTable));
		if(obj.costJoin>cost || ((obj.costJoin==0) && cost>0))
        {
			obj.costJoin=cost;
			obj.JType="SMJ buffer = " + bufferMemory;
			obj.isinReverse=false;
			isSortergeDone=true;
		}
	    return cost;
    }

    public static long CostOfhashJoinWithBuffer(int leftTable, int rightTable, CostOfData obj)
    {
        int bufferMemory = buffMemWB;
        long cost;
        double multiplier = Math.log10((leftTable+rightTable)/(bufferMemory-1))/(Math.log10(bufferMemory-1));
        cost = (int)Math.ceil(2*(leftTable+rightTable)*(1+multiplier) + leftTable + rightTable);
        if(obj.costJoin>cost || ((obj.costJoin==0) && cost>0))
        {
            obj.costJoin=cost;
            obj.JType = "Hash Join buffer = " + bufferMemory ;
        }
        return cost;
    }

    public static long CostOfhashJoinWithLessBuffer(int leftTable, int rightTable, CostOfData obj)
    {
        int bufferMemory = buffMemlB;
        double multiplier = Math.log10((leftTable+rightTable)/(bufferMemory-1))/(Math.log10(bufferMemory-1));
        long cost;
        cost = (int)Math.ceil(2*(leftTable+rightTable)*(1+multiplier) + leftTable + rightTable);   	
        if(obj.costJoin>cost || ( (obj.costJoin==0) && cost>0))
        {
            obj.costJoin=cost;
            obj.JType = "Hash Join buffer = " + bufferMemory;
        }
        return cost;
    }

    private static void display(long cost1, long cost2)
    {
        if(cost1>cost2)
        {
            System.out.println("-----------------------------------------------------------\n");
            System.out.println("RQ1 is the Best Query Plan because the Cost of RQ1 < Cost of Q1.\n");
        }
        else
        {
            System.out.println("-----------------------------------------------------------\n");
            System.out.println("Q1 is the Best Query Plan because Cost of RQ1 > Cost of Q1\n");
        }	
    }
}
