#!/bin/bash
continue=1
while [ $continue == 1 ]; do
echo "-------Modules--------"
echo "1.Sum Of Masses"
echo "2.Sum of Charge"
echo "3.Moment of Inertia"
echo "4.Moment of Inertia over Z-axis"
echo "5.Center of Mass"
echo "6.Dipole Moment"
echo "Enter your Choice and press [Enter] : "
read choice
case $choice in 
1)spark-submit --class "som" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/SOM.txt
;;
2)spark-submit --class "soq" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/SOQ.txt
;;
3)spark-submit --class "moi" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/MOI.txt
;;
4)spark-submit --class "moiz" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/MOIZ.txt
;;
5)spark-submit --class "com" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/COM.txt
;;
6)spark-submit --class "dpm" --master local[4] /home/surajnakka/MSDataAnalysis/target/scala-2.10/ms-data-analysis_2.10-1.0.jar
  gedit /home/surajnakka/MSDataAnalysis/output/DPM.txt
;;
*) echo "Please enter correct choice"
;;
esac
echo "Do you want to continue ?[Y:1,N=0]"
echo "Enter your Choice and press [Enter] : "
read cont_choice
case $cont_choice in
0) 
   continue=0
   clear
;;
1) clear
;;
*) echo "Wrong Choice Entered...exiting"
   continue=0
   clear
;;
esac
done  
exit 
