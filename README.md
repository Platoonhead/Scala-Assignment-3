# Scala-Assignment-3
assigned : 27th jan 2017 (friday), lastdate : 30th jan 2017 (monday) by himanshu 
Scala Assignment 3
==================================================
Problem statement
==================================================
Design a simple ETL process – Extract Transform and Load

First Part
Read files from a directory
For each file read contents of the file
Capitalize the contents of the file 
Then write the capitalized content into another output file with the same name in a different directory

Second Part:
Read all the unique words from the file
For each word calculate the word count 
For eg: for a file containing the content
“Hello world, hello”
In this case the summary would be 
hello -> 2
world -> 1
For simplicity cases should be ignored – Hello and hello mean the same word
This summary should then be added to an output file with the same name in a different directory 

Design Considerations:
It should be easy to add or remove transformations
Input source could change i.e. –
In future we could read contents from a SQL table
Output source could also change i.e – 
In future we could write contents to a SQL table

Expectations:
1. You are required to help create the software that works according to the sample inputs applied and generates output as shown in the Input-Output section shown for the problem.
2. You should demonstrate the working software by building a console application or writing test program that exercises the sample inputs, for this purpose you may use jUnit or any other testing framework.
3. Maturity of your solution will be judged on your object oriented analysis and design skills. Your coding skills will also be judged.
4. Please note that the problems below do not require any relational database or knowledge of underlying OS or Windowing platform.
