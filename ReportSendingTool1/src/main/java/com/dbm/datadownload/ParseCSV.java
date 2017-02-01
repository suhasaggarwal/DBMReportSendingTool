package com.dbm.datadownload;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

public class ParseCSV{

    public static void main(String[] args) throws Exception {
        String splitBy = ",";
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\sony\\Desktop\\test.csv"));
       
        
       
        File fout = null;
        fout = new File("C:\\Users\\sony\\Desktop\\processedtest.csv");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fout);
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        
        
        
        String[] b;
        String splitby1;
        String splitby2;
        String[] c;
        String csvline;
        String line;
        
        while(true){
         
        line = br.readLine();
        
        if(line!=null && line.isEmpty()!=true){
             b = line.split(splitBy);
             System.out.println(b[0]);
             splitby1 = " ";
             splitby2 = ".";
             if(b[0].contains(splitby1)){
             c = b[0].split(splitby1);
             csvline = c[0]+","+b[1]+","+b[2]+","+c[c.length-1];
             }
             
             if(line.contains(splitby2)){
                 c = b[0].split(splitby2);
                 csvline = c[0]+","+b[1]+","+b[2]+","+c[c.length-1];
                 }
             
             bw.write(line);
             bw.newLine();
             
        }
        else
        	break;
        }
        br.close();
        bw.close();
  }
}
