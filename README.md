Automation for merging multiple csv files into a single dataframe,Fetch the required data fields from the merged dataframe then upload it into database created. 


Note: 
  1.change the file names and req_columns value as per your need 
  2.If root user is'nt working create new SQLserver
        
        #Script for creating new SQLserver and giving privileges to the newly created SQLserver  
        sudo mysql -u root
        CREATE 'user_name'@'localhost' IDENTIFIED BY 'password'
        GRANT ALL PRIVILEGES ON *.* TO 'charan'@'localhost';
        FLUSH PRIVILEGES;
