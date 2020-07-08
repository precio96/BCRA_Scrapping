########################################################################################################
#                                     Extraer data de la pagina del BCRA                               #
########################################################################################################

# Inicio ------------------------------------------------------------------

#Instalamos los paquetes necesarios

#install.packages("xml2")
#install.packages("rvest")
#install.packages("RODBC")
#install.packages("odbc")
#install.packages("sqldf")

#Importamos las librerias

library("xml2")
library("rvest")
library("RODBC")
library("odbc")
library("sqldf")

#Conectamos al SQL
connection <- DBI::dbConnect(odbc::odbc(),
                             Driver    = "SQL Server", 
                             Server    = "DB_IP",
                             Database  = "DB_Name",
                             UID       = "DB_User",
                             PWD       = "DB_PWD",
                             Port      = "DB_Port")

#Definimos la pagina
url <- "https://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables.asp"   

#Limpiamos la pagina para obtener solo la tabla principal
webpage <- xml2::read_html(url)
BCRA <- rvest::html_table(webpage)[[1]] %>% tibble::as_tibble(.name_repair = "unique")

# CER ------------------------------------------------------------------

#Me quedo solo con el valor del CER
pos_CER <- lapply(BCRA, function(ch) grep("CER", ch,ignore.case =  T))
CER<- BCRA[pos_CER$X1,2:3]
names(CER) <- c("Fecha","Valor")

#CAMBIO EL FORMATO DE DECIMAL = "," A DECIMAL = "."
CER$Valor<-as.numeric(gsub(",", ".", gsub("\\.", "", CER$Valor)))
CER$Fecha <- as.Date(CER$Fecha,"%d/%m/%Y")

#Importo la ultima fecha cargada para ver si hay un valor nuevo
max_fecha_cer <- dbGetQuery(connection, "SELECT MAX(FECHA) FROM tabla_CER")
names(max_fecha_cer) <- "Max_Fecha"


if(max_fecha_cer$Max_Fecha!=CER$Fecha){
  #Cargamos la tabla diaria del CER
  dbWriteTable(conn = connection, 
               name = "DB_TABLE1", 
               value = CER,
               append = TRUE)
  log_CER<-"CER cargado correctamente!"
  print(log_CER)
  } else {
  log_CER<-"No se pudo cargar el CER!"
  print(log_CER)
  }

# BADLAR ------------------------------------------------------------------

#Me quedo con todas las TNAs
pos_TNA <- lapply(BCRA, function(ch) grep("(en % n.a.)", ch,ignore.case =  T))
TNA<- BCRA[pos_TNA$X1,]

#Me quedo solo con el valor de la tasa BADLAR
pos_BADLAR <- lapply(TNA, function(ch) grep("BADLAR", ch,ignore.case =  T))
BADLAR <-  TNA[pos_BADLAR$X1,2:3]
names(BADLAR) <- c("Fecha","Valor")

#CAMBIO EL FORMATO DE DECIMAL = "," A DECIMAL = "."
BADLAR$Valor<-as.numeric(gsub(",", ".", gsub("\\.", "", BADLAR$Valor)))
BADLAR$Fecha <- as.Date(BADLAR$Fecha,"%d/%m/%Y")

#Importo la ultima fecha cargada para ver si hay un valor nuevo
max_fecha_BADLAR <- dbGetQuery(connection, "SELECT MAX(FECHA) FROM tabla_BADLAR")
names(max_fecha_BADLAR) <- "Max_Fecha"


if(max_fecha_BADLAR$Max_Fecha!=BADLAR$Fecha){
  #Cargamos la tabla diaria del CER
  dbWriteTable(conn = connection, 
               name = "DB_TABLE2", 
               value = BADLAR,
               append = TRUE)
  log_BADLAR<-"BADLAR cargado correctamente!"
  print(log_BADLAR)
  
  } else {
  log_BADLAR<-"No se pudo cargar la BADLAR!"
  print(log_BADLAR)
}

# TASA DE ADELANTOS ------------------------------------------------------------------

#Me quedo solo con el valor de la tasa de Adelantos
pos_ADELANTOS <- lapply(BCRA, function(ch) grep("Tasa de inter?s de pr?stamos por adelantos en cuenta corriente", ch,ignore.case =  T))
ADELANTOS <-  BCRA[pos_ADELANTOS$X1,2:3]
names(ADELANTOS) <- c("Fecha","Valor")

#CAMBIO EL FORMATO DE DECIMAL = "," A DECIMAL = "."
ADELANTOS$Valor<-as.numeric(gsub(",", ".", gsub("\\.", "", ADELANTOS$Valor)))
ADELANTOS$Fecha <- as.Date(ADELANTOS$Fecha,"%d/%m/%Y")

#Importo la ultima fecha cargada para ver si hay un valor nuevo
max_fecha_ADELANTOS <- dbGetQuery(connection, "SELECT MAX(FECHA) FROM tabla_ADELANTOS")
names(max_fecha_ADELANTOS) <- "Max_Fecha"


if(max_fecha_ADELANTOS$Max_Fecha!=ADELANTOS$Fecha){
  #Cargamos la tabla diaria del CER
  dbWriteTable(conn = connection, 
               name = "DB_TABLE3", 
               value = ADELANTOS,
               append = TRUE)
  log_ADELANTOS<-"Tasa de Adelantos cargada correctamente!"
  print(log_ADELANTOS)
} else {
  log_ADELANTOS <- "No se pudo cargar la Tasa de Adelantos!"
  print(log_ADELANTOS)
  }

write.table(c(log_CER,format(CER$Fecha,"%d/%m/%Y"),CER$Valor
              ,log_BADLAR,format(BADLAR$Fecha,"%d/%m/%Y"),BADLAR$Valor
              ,log_ADELANTOS,format(ADELANTOS$Fecha,"%d/%m/%Y"),ADELANTOS$Valor)
            ,file=paste0("carga_tasas -",Sys.Date(),".txt"))

print("Proceso completo!")
