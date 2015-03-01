ClearAndInitialize <- function(){
        # Clear all:        
        rm(list=ls())
        
        library (dplyr)
        
        path<-"~/BusinessIntelligence/GitHub/Neo4j_import/dataset/"
        
        loadCrdNode <- function(){                
                ## set filenames
                fileIn<-"neo4j_script_crd.txt"
                fileOut<-"CrdNode.txt"
                fileHeaderOut<-"CrdNode-header.txt"
                
                ## create column names for df
                CrdNode.colnames <- c("Card_Id",
                                      "Number_Of_Authorizations",
                                      "Number_Of_Fraud_Transactions",
                                      "Fraud_Rate", 
                                      "Start_DateTime",
                                      "End_DateTime")
                
                ## create separate header names
                CrdNode.header <- c("Card_Id:ID,Number_Of_Authorizations:int,Number_Of_Fraud_Transactions:int,Fraud_Rate:float,Start_DateTime,End_DateTime,:LABEL")
                
                ## Load Cards        
                CrdNode <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=CrdNode.colnames)
                
                ## add label
                CrdNode<-dplyr::mutate(CrdNode,":LABEL"="CARD")
                
                ## write the cards        
                write.table (CrdNode, file=fileOut, sep=",", col.names=FALSE,row.names=FALSE)
                
                ## write the cards header
                write.table (CrdNode.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadAutNode <- function(){
                fileIn<-"neo4j_script_aut.txt"
                fileOut<-"AutNode.txt"
                fileHeaderOut<-"AutNode-header.txt"
                
                ## create column names for df
                AutNode.colnames <- c("Authorization_Id",
                                      "Number_Of_Authorizations",
                                      "Number_Of_Fraud_Transactions",
                                      "Fraud_Rate", 
                                      "Start_DateTime",
                                      "End_DateTime")      
                
                ## create separate header names
                AutNode.header<-c("Authorization_Id:ID,Number_Of_Authorizations:int,Number_Of_Fraud_Transactions:int,Fraud_Rate:float,Start_DateTime,End_DateTime,:LABEL")        
                
                ## Load Autorizations
                AutNode <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=AutNode.colnames)     
                
                ## add label
                AutNode<-dplyr::mutate(AutNode,":LABEL"="AUTHORIZATION")
                
                ## write the authorizations
                write.table (AutNode, file=fileOut, sep=",", col.names=FALSE,row.names=FALSE)
                
                ## write the authorizations header
                write.table (AutNode.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadAcpNode <- function(){                
                ## set filenames
                fileIn<-"neo4j_script_acp.txt"
                fileOut<-"AcpNode.txt"
                fileHeaderOut<-"AcpNode-header.txt"
                
                ## create column names for df
                AcpNode.colnames <- c("Acceptor_Id",
                                      "Number_Of_Authorizations",
                                      "Number_Of_Fraud_Transactions",
                                      "Fraud_Rate", 
                                      "Start_DateTime",
                                      "End_DateTime")
                
                ## create separate header names
                AcpNode.header <- c("Acceptor_Id:ID,Number_Of_Authorizations:int,Number_Of_Fraud_Transactions:int,Fraud_Rate:float,Start_DateTime,End_DateTime,:LABEL")
                
                ## Load Acceptor
                AcpNode <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=AcpNode.colnames)
                
                ## add label
                AcpNode<-dplyr::mutate(AcpNode,":LABEL"="ACCEPTOR")
                
                ## write the acceptor        
                write.table (AcpNode, file=fileOut, sep=",", col.names=FALSE,row.names=FALSE)
                
                ## write the acceptor header
                write.table (AcpNode.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadCrdAutRel <- function() {
                ## set filenames
                fileIn<-"neo4j_script_aut_crd.txt"
                fileOut<-"CrdAutRel.txt"
                fileHeaderOut<-"CrdAutRel-header.txt"
                
                
                ## create column names for df   
                CrdAutRel.colnames <- c("Authorization_Id",
                                        "Card_Id",
                                        "Fraud",
                                        "Number2",
                                        "AutCrd_DateTime",
                                        "End_DateTime",
                                        "Number3")
                
                ## create separate header names
                CrdAutRel.header<-c(":START_ID,:END_ID,Fraud:int,AutCrd_DateTime,:TYPE")
                
                ## trick to read only selected columns
                CrdAutRel.colClasses <- rep("NULL", 7)
                CrdAutRel.colClasses[c(1,2,3,5)] <- NA
                
                ## Load Relations
                CrdAutRel <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=CrdAutRel.colnames,colClasses=CrdAutRel.colClasses)
                
                ## FIX ERROR IN SOURCE!!!
                CrdAutRel$Card_Id<-substr(CrdAutRel$Card_Id,1,nchar(CrdAutRel$Card_Id)-1)
                
                ## add TYPE
                CrdAutRel<-dplyr::mutate(CrdAutRel,":TYPE"="crd_aut")
                
                ## write the CrdAut Relations
                write.table (CrdAutRel, file=fileOut,sep=",",col.names=FALSE,row.names=FALSE)
                
                ## write the CrdAut Relations
                write.table (CrdAutRel.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)
                
        }
        
        loadAutAcpRel <- function() {
                ## set filenames
                fileIn<-"neo4j_script_aut_acp.txt"
                fileOut<-"AutAcpRel.txt"
                fileHeaderOut<-"AutAcpRel-header.txt"
                
                ## create column names for df   
                AutAcpRel.colnames <- c("Authorization_Id",
                                        "Acceptor_Id",
                                        "Number1",
                                        "Number2",
                                        "Fraud",
                                        "Number3",
                                        "AutAcp_DateTime",
                                        "End_DateTime",
                                        "Number4")
                
                ## create separate header names
                AutAcpRel.header<-c(":START_ID,:END_ID,Fraud:int,AutAcp_DateTime,:TYPE")
                
                ## trick to read only selected columns
                AutAcpRel.colClasses <- rep("NULL", 9)
                AutAcpRel.colClasses[c(1,2,5,7)] <- NA
                
                ## Load Relations
                AutAcpRel <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=AutAcpRel.colnames,colClasses=AutAcpRel.colClasses)        
                
                ## add TYPE
                AutAcpRel<-dplyr::mutate(AutAcpRel,":TYPE"="aut_acp")
                
                ## write the AutAcp Relations
                write.table (AutAcpRel, file=fileOut,sep=",",col.names=FALSE,row.names=FALSE)
                
                ## write the AutAcp Relations header
                write.table (AutAcpRel.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadActNode <- function(){                
                ## set filenames
                fileIn<-"neo4j_script_act.txt"
                fileOut<-"ActNode.txt"
                fileHeaderOut<-"ActNode-header.txt"
                
                ## create column names for df
                ActNode.colnames <- c("Account_Id",
                                      "Number_Of_Authorizations",
                                      "Number_Of_Fraud_Transactions",
                                      "Fraud_Rate", 
                                      "Start_DateTime",
                                      "End_DateTime")
                
                ## create separate header names
                ActNode.header <- c("Account_Id:ID,Number_Of_Authorizations:int,Number_Of_Fraud_Transactions:int,Fraud_Rate:float,Start_DateTime,End_DateTime,:LABEL")
                
                ## Load Accounts        
                ActNode <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=ActNode.colnames)
                
                ## add label
                ActNode<-dplyr::mutate(ActNode,":LABEL"="ACCOUNT")
                
                ## write the accounts        
                write.table (ActNode, file=fileOut, sep=",", col.names=FALSE,row.names=FALSE)
                
                ## write the acounts header
                write.table (ActNode.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadMccNode <- function(){                
                ## set filenames
                fileIn<-"neo4j_script_mcc.txt"
                fileOut<-"MccNode.txt"
                fileHeaderOut<-"MccNode-header.txt"
                
                ## create column names for df
                MccNode.colnames <- c("MCC_Id",
                                      "Number_Of_Authorizations",
                                      "Number_Of_Fraud_Transactions",
                                      "Fraud_Rate", 
                                      "Start_DateTime",
                                      "End_DateTime")
                
                ## create separate header names
                MccNode.header <- c("MCC_Id:ID,Number_Of_Authorizations:int,Number_Of_Fraud_Transactions:int,Start_DateTime,End_DateTime,:LABEL")
                
                ## trick to read only selected columns
                MccNode.colClasses <- rep("NULL", 6)
                MccNode.colClasses[c(1,2,3,5,6)] <- NA
                
                ## Load MCC's        
                MccNode <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=MccNode.colnames,colClasses=MccNode.colClasses)
                
                ## add label
                MccNode<-dplyr::mutate(MccNode,":LABEL"="MCC")
                
                ## write the accounts        
                write.table (MccNode, file=fileOut, sep=",", col.names=FALSE,row.names=FALSE)
                
                ## write the acounts header
                write.table (MccNode.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)        
        }
        
        loadCrdActRel <- function() {
                ## set filenames
                fileIn<-"neo4j_script_crd_act.txt"
                fileOut<-"CrdActRel.txt"
                fileHeaderOut<-"CrdActRel-header.txt"
                
                ## create column names for df   
                CrdActRel.colnames <- c("Card_Id",
                                        "Account_Id",
                                        "Number1",
                                        "Fraud",
                                        "Number2",
                                        "Start_DateTime",
                                        "End_DateTime",
                                        "Number3")
                
                ## create separate header names
                CrdActRel.header<-c(":START_ID,:END_ID,Fraud:int,:TYPE")
                
                ## trick to read only selected columns
                CrdActRel.colClasses <- rep("NULL", 8)
                CrdActRel.colClasses[c(1,2,3)] <- NA
                
                ## Load Relations
                CrdActRel <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=CrdActRel.colnames,colClasses=CrdActRel.colClasses)        
                
                ## add TYPE
                CrdActRel<-dplyr::mutate(CrdActRel,":TYPE"="crd_act")
                
                ## write the CrdAct Relations
                write.table (CrdActRel, file=fileOut,sep=",",col.names=FALSE,row.names=FALSE)
                
                ## write the CrdAct Relations header
                write.table (CrdActRel.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)
                
        }
        
        loadMccAcpRel <- function() {
                ## set filenames
                fileIn<-"neo4j_script_mcc_acp.txt"
                fileOut<-"MccAcpRel.txt"
                fileHeaderOut<-"MccAcpRel-header.txt"
                
                ## create column names for df   
                MccAcpRel.colnames <- c("MCC_Id",
                                        "Acceptor_Id",
                                        "Number_Of_Authorizations",
                                        "Fraud",
                                        "Number2",
                                        "Start_DateTime",
                                        "End_DateTime",
                                        "Number3")
                
                ## create separate header names
                MccAcpRel.header<-c(":START_ID,:END_ID,Number_Of_Authorizations:int,Fraud:int,:TYPE")
                
                ## trick to read only selected columns
                MccAcpRel.colClasses <- rep("NULL", 8)
                MccAcpRel.colClasses[c(1,2,3,4)] <- NA
                
                ## Load Relations
                MccAcpRel <- read.csv(paste(path,fileIn,sep=''), header=FALSE, stringsAsFactors=FALSE, col.names=MccAcpRel.colnames,colClasses=MccAcpRel.colClasses)        
                
                ## add TYPE
                MccAcpRel<-dplyr::mutate(MccAcpRel,":TYPE"="mcc_acp")
                
                ## write the MccAcp Relations
                write.table (MccAcpRel, file=fileOut,sep=",",col.names=FALSE,row.names=FALSE)
                
                ## write the CrdAct Relations header
                write.table (MccAcpRel.header, file=fileHeaderOut, sep="", col.names=FALSE,row.names=FALSE,quote=FALSE)
                
        }
        
        
        
        
        loadCrdNode()
        loadAutNode()
        loadAcpNode()
        loadActNode()
        loadMccNode()
        loadCrdAutRel()
        loadAutAcpRel()
        loadCrdActRel()
        loadMccAcpRel()
        
        # Clear all:        
        rm(list=ls())
}