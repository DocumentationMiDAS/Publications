# Copyright (C). 2025

# Auteures: Poppée Mongruel, Clara Ponton. DARES

# Ce programme informatique a été développé par la DARES.
# Il permet de produire les illustrations et les résultats de la publication N°38 de la collection Dares FOCUS "Bénéficiaires du RSA et inscription à France Travail : parcours à horizon d'un an"

# Le texte et les tableaux peuvent être consultés sur le site de la Dares : https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail

# Ce programme utilise les données Midas de juin 2022 à juin 2023. 
# Dans les données utilisées, les bénéficiaires du RSA sont repérés par leur identifiant MIDAS, qui n'est diffusé que via le CASD

# Bien qu'ils n'existent aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler à la DARES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils y rencontreraient.
# Contact : contact.dares.dmq@travail.gouv.fr

# Ce programme a été exécuté pour la dernière fois le 01/07/2025 avec la version 4.5.1 de R.

rm(list=ls())

#### Import des librairies #### ------
librairie <- c("knitr","xlsx","lubridate","haven","here","dplyr","tidyr","ggplot2","writexl",
               "kableExtra","arrow","duckdb","collapse","rbenchmark","purrr","sparklyr",
               "dbplyr", "janitor", "reshape2")

#Installation des librairies manquantes 
installed_librairie <- librairie %in% rownames(installed.packages())
if(any(installed_librairie = FALSE)){
  install.packages(librairie[!installed_librairie])
}

# Chargement des librairies 
invisible(lapply(librairie,library,character.only = T))

# repertoires
dir_spark <- "file:///C:/Users/Public/Documents/MiDAS_parquet/Vague 4"

### Dates ------------
mois <- "2022-06-01" # on s'intéresse aux BRSA en juin 2022
mois_m <- paste0(substr(mois,1,4),substr(mois,6,7))

#### Creation des dates de debut et fin de mois ####
debut_fin_mois <- data.frame(debut_mois = seq.Date(as.Date("2010/01/01"),
                                                   as.Date("2030/01/01"),
                                                   by = "+1 months")) %>%
  mutate(fin_mois = (debut_mois + months(1)) -1,
         mois = paste0(substr(debut_mois,1,4),substr(debut_mois,6,7)),
         mois_suivant = dplyr::lead(mois),
         mois_precedent = lag(mois))

#### Spark ####
conf <- spark_config()
conf$`sparklyr.cores.local` <- 10  
conf$`sparklyr.shell.driver-memory` <- "150G"  # memoire allouee a l'unique executeur en local
conf$spark.storage.memoryFraction <- "0.1"    # fraction de la memoire allou?e au sockage
conf$spark.sql.shuffle.partitions <- "200"     
conf$spark.driver.maxResultSize <- 0           

### Lancer la connexion Spark
sc <- spark_connect(master="local",config=conf)


### 1. Retour à l'emploi sur un an des BRSA selon qu'ils sont inscrits ou non à France Traval ----------

# base des BRSA en juin 22
de_ms <- spark_read_parquet(sc, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms_DF3.parquet", memory=FALSE)

# créer des listes de bRSA pour chaque mois à partir de juin 2022
for (i in 0:12){
  
  mois_cnaf <- paste0(substr(ymd(as.Date(mois))+months(i),6,7), substr(ymd(as.Date(mois))+months(i),3,4))
  
  name <- paste0('brsa_',mois_cnaf)
  
  assign(name , spark_read_parquet(sc, memory=FALSE,
                                   paste0(dir_spark, "/Allstat/minsoc/cnaf_minsoc_", mois_cnaf, ".parquet")) %>%
           dplyr::select(id_midas, RSAVERS, MTRSAVER) %>%
           dplyr::filter(RSAVERS %in% c("RSA droit commun", "RSA droit local",
                                        "RSA jeune", "RSA expérimental", "C", "L", "J", "E")) %>%
           dplyr::filter(MTRSAVER > 0) %>%
           # sélectionner uniquement les BRSA qui perçoivent une allocation non nulle
           window_order(id_midas) %>%
           group_by(id_midas) %>%
           filter(row_number() == 1) %>% # Dédoublonne
           ungroup() %>%
           dplyr::select(id_midas) %>% collect())
  
}

# liste de bRSA pour les mois antérieurs 
for (i in -6:0){

  mois_cnaf <- paste0(substr(ymd(as.Date(mois))+months(i),6,7), substr(ymd(as.Date(mois))+months(i),3,4))

  name <- paste0('brsa_',mois_cnaf)

  assign(name , spark_read_parquet(sc, memory=FALSE,
                                   paste0(dir_spark, "/Allstat/minsoc/cnaf_minsoc_", mois_cnaf, ".parquet")) %>%
           dplyr::select(id_midas, RSAVERS, MTRSAVER) %>%
           dplyr::filter(RSAVERS %in% c("RSA droit commun", "RSA droit local",
                                        "RSA jeune", "RSA expérimental", "C", "L", "J", "E")) %>%
           dplyr::filter(MTRSAVER > 0) %>%
           # sélectionner uniquement les BRSA qui perçoivent une allocation non nulle
           window_order(id_midas) %>%
           group_by(id_midas) %>%
           filter(row_number() == 1) %>% # Dédoublonne
           ungroup() %>%
           dplyr::select(id_midas))

}

# base mouvements de main d'oeuvre 2022 et 2023
mmo_22 <- spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/MMO/mmo_2022.parquet")) %>%
  dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, 
                L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>%
  dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                  is.na(id_midas) == FALSE) %>%
  filter(Quali_Salaire_Base == "7")

mmo_23 <- spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/MMO/mmo_2023.parquet")) %>%
  dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, 
                L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>%
  dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                  is.na(id_midas) == FALSE) %>%
  filter(Quali_Salaire_Base == "7")

fin_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))
debut_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(debut_mois))

# base : bRSA sans emploi en juin 22
BRSA_sans_emploi_0622 <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  left_join(mmo_22 %>% 
              dplyr::filter(DebutCTT <= fin_mois & 
                              (FinCTT > fin_mois | is.na(FinCTT) == TRUE | FinCTT == "")) %>%
              dplyr::filter(Quali_Salaire_Base == "7") %>%
              dplyr::mutate(
                type_contrat = case_when(
                  Nature %in% c("01","09","50","82","91")~"CDI",
                  Nature %in% c("02","07","10","20","21","32","51",
                                "52","80","81","92","93") ~ "CDD",
                  Nature %in% c("03", "08") ~ "interim",
                  Nature %in% c("29", "53", "60", "70", "89", "90") ~ "autre",
                  TRUE ~ NA),
                ordre_contrat = case_when(
                  type_contrat == "CDI" ~ 1,
                  type_contrat == "CDD" ~ 2,
                  type_contrat == "interim" ~ 3,
                  type_contrat == "autre" ~ 4,
                  TRUE ~ NA
                ) )  %>% 
              dplyr::add_count(id_midas) %>%
              dplyr::rename(nb_contrat_ind = n) %>%
              dbplyr::window_order(id_midas, ordre_contrat) %>%
              dplyr::group_by(id_midas) %>% 
              dplyr::filter(row_number() == 1) %>% # garder un contrat par ind en priorisant les CDI, puis CDD..
              ungroup,
            by = "id_midas") %>%
  dplyr::filter(is.na(type_contrat)) %>%
  dplyr::select(id_midas, DE) %>% # ajout variable entrant sur le trimestre
  left_join(brsa_0122 %>% dplyr::mutate(m1 = 1), by="id_midas") %>%
  left_join(brsa_0222 %>% dplyr::mutate(m2 = 1), by="id_midas") %>%
  left_join(brsa_0322 %>% dplyr::mutate(m3 = 1), by="id_midas") %>%
  dplyr::mutate(entrant_trimestre = ifelse(is.na(m1) & is.na(m2) & is.na(m3), "entrant_trim", "stock")) # entrant sur le trimestre T2 : ne pas avoir ete brsa au T1 (janv, fev, mars)


### 2. Persistance au RSA sur un an -----------

# BRSA de juin qui sont au RSA 1m, 2m, ... 12 mois plus tard
persistance_rsa <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  collect() %>%
  mutate(BRSA_1m = ifelse(id_midas %in% brsa_0722$id_midas,1,0),
         BRSA_2m = ifelse(id_midas %in% brsa_0822$id_midas,1,0), 
         BRSA_3m = ifelse(id_midas %in% brsa_0922$id_midas,1,0), 
         BRSA_4m = ifelse(id_midas %in% brsa_1022$id_midas,1,0), 
         BRSA_5m = ifelse(id_midas %in% brsa_1122$id_midas,1,0), 
         BRSA_6m = ifelse(id_midas %in% brsa_1222$id_midas,1,0), 
         BRSA_7m = ifelse(id_midas %in% brsa_0123$id_midas,1,0), 
         BRSA_8m = ifelse(id_midas %in% brsa_0223$id_midas,1,0), 
         BRSA_9m = ifelse(id_midas %in% brsa_0323$id_midas,1,0), 
         BRSA_10m = ifelse(id_midas %in% brsa_0423$id_midas,1,0), 
         BRSA_11m = ifelse(id_midas %in% brsa_0523$id_midas,1,0), 
         BRSA_12m = ifelse(id_midas %in% brsa_0623$id_midas,1,0) ) %>%
  dplyr::mutate(duree_RSA_1an = BRSA_1m + BRSA_2m + BRSA_3m + BRSA_4m + BRSA_5m + 
                  BRSA_6m + BRSA_7m + BRSA_8m + BRSA_9m + BRSA_10m + BRSA_11m + BRSA_12m)


### 3. Persistance a France Travail sur un an -----------

# créer des listes de demandeurs d'emploi pour chaque mois à partir de juin 2022
for (i in 0:12){
  
  mois_m <- paste0(substr(ymd(as.Date(mois))+months(i),1,4), substr(ymd(as.Date(mois))+months(i),6,7))
  fin_mois_m <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))
  debut_mois_m <- pull(debut_fin_mois %>% filter(mois == mois_m) %>%  select(debut_mois))
  num_mois <- paste0(substr(ymd(as.Date(mois))+months(i),6,7), substr(ymd(as.Date(mois))+months(i),3,4))
  
  name <- paste0('de_',num_mois)
  
  assign(name, spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/FHS/de.parquet")) %>%
           dplyr::mutate(DATINS = as.Date(DATINS),
                         DATANN = as.Date(DATANN)) %>%
           dplyr::filter(DATINS <= fin_mois_m & (is.na(DATANN) | DATANN > fin_mois_m)) %>% # demandeurs d'emploi en fin de mois au mois m
           window_order(id_midas, DATINS) %>%
           group_by(id_midas) %>%
           filter(row_number() == 1) %>% 
           ungroup() %>% # 1 obs par individu, en gardant la date d'inscription la plus récente
           dplyr::select(id_midas, DATINS) %>% collect())
  
}

# BRSA de juin qui sont inscrits à France Travail 1m, 2m, ... 12 mois plus tard
persistance_ft <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  collect() %>%
  mutate(de_1m = ifelse(id_midas %in% de_0722$id_midas,1,0),
         de_2m = ifelse(id_midas %in% de_0822$id_midas,1,0), 
         de_3m = ifelse(id_midas %in% de_0922$id_midas,1,0), 
         de_4m = ifelse(id_midas %in% de_1022$id_midas,1,0), 
         de_5m = ifelse(id_midas %in% de_1122$id_midas,1,0), 
         de_6m = ifelse(id_midas %in% de_1222$id_midas,1,0), 
         de_7m = ifelse(id_midas %in% de_0123$id_midas,1,0), 
         de_8m = ifelse(id_midas %in% de_0223$id_midas,1,0), 
         de_9m = ifelse(id_midas %in% de_0323$id_midas,1,0), 
         de_10m = ifelse(id_midas %in% de_0423$id_midas,1,0), 
         de_11m = ifelse(id_midas %in% de_0523$id_midas,1,0), 
         de_12m = ifelse(id_midas %in% de_0623$id_midas,1,0) ) %>%
  dplyr::mutate(duree_ft_1an = de_1m + de_2m + de_3m + de_4m + de_5m + 
                  de_6m + de_7m + de_8m + de_9m + de_10m + de_11m + de_12m)

#### CROISEMENT RSA-FT-CONTRAT ------------

#on joint les trois base sur le temps passé en contrat, au RSA et à FT

fin_mois_h <- as.Date("2023-06-30")

persistance_ft_rsa <- sparklyr::copy_to(sc, right_join(persistance_ft,persistance_rsa, by = 'id_midas'), overwrite = TRUE)

persistance_ft_rsa <- persistance_ft_rsa  %>%
  left_join(mmo_22 %>% 
              dplyr::filter(DebutCTT <= as.Date("2022-06-30") & 
                              (FinCTT > as.Date("2022-06-30") | is.na(FinCTT) == TRUE | FinCTT == "")) %>%
              dplyr::filter(Quali_Salaire_Base == "7") %>%
              dplyr::mutate(
                type_contrat = case_when(
                  Nature %in% c("01","09","50","82","91")~"CDI",
                  Nature %in% c("02","07","10","20","21","32","51",
                                "52","80","81","92","93") ~ "CDD",
                  Nature %in% c("03", "08") ~ "interim",
                  Nature %in% c("29", "53", "60", "70", "89", "90") ~ "autre",
                  TRUE ~ NA),
                ordre_contrat = case_when(
                  type_contrat == "CDI" ~ 1,
                  type_contrat == "CDD" ~ 2,
                  type_contrat == "interim" ~ 3,
                  type_contrat == "autre" ~ 4,
                  TRUE ~ NA
                ) )  %>% 
              dplyr::add_count(id_midas) %>%
              dplyr::rename(nb_contrat_ind = n) %>%
              dbplyr::window_order(id_midas, ordre_contrat) %>%
              dplyr::group_by(id_midas) %>% 
              dplyr::filter(row_number() == 1) %>% # garder un contrat par ind en priorisant les CDI, puis CDD..
              ungroup %>% select(id_midas, type_contrat),
            by = "id_midas") %>%
  dplyr::mutate(Emploi0622 = ifelse(is.na(type_contrat),0,1)) %>%
  left_join(mmo_23 %>% 
              select(emploi_bit_2023_06, id_midas, DebutCTT, FinCTT, Nature, ModeExercice) %>%
              rename(emploi_bit = 1) %>%
              dplyr::mutate(
                DebutCTT = as.date(DebutCTT),
                FinCTT = as.date(FinCTT),
                type_contrat = case_when(
                  Nature %in% c("01","09","50","82","91")~"CDI",
                  Nature %in% c("02","07","10","20","21","32","51",
                                "52","80","81","92","93") ~ "CDD",
                  Nature %in% c("03", "08") ~ "interim",
                  Nature %in% c("29", "53", "60", "70", "89", "90") ~ "autre",
                  TRUE ~ NA),
                ordre_contrat = case_when(
                  type_contrat == "CDI" ~ 1,
                  type_contrat == "CDD" ~ 2,
                  type_contrat == "interim" ~ 3,
                  type_contrat == "autre" ~ 4,
                  TRUE ~ NA)) %>%
              dplyr::mutate(FinCTT = ifelse(is.na(FinCTT), as.date(fin_mois_h), FinCTT)) %>%  # si pas de date fin contrat, remplacer par la fin periode interet
              dplyr::mutate(    
                duree_contrat = DATEDIFF(FinCTT, DebutCTT)+1, # duree du contrat : entre date debut contrat et date fin contrat
                temps_partiel = ifelse(ModeExercice %in%  c("20", "30", "41", "42", "40"), 1, 0), 
                temps_complet = ifelse(ModeExercice == "10", 1, 0)) %>% 
              dbplyr::window_order(id_midas, ordre_contrat, desc(duree_contrat)) %>%
              dplyr::group_by(id_midas) %>% 
              dplyr::filter(row_number() == 1) %>% # garder un contrat par ind en priorisant les CDI, puis CDD.. (pour observer les contrats en cours à fin mois)
              ungroup %>% 
              dplyr::mutate(contrat_encours = case_when(
                type_contrat == "CDD" & (duree_contrat >= 180 | is.na(duree_contrat) == TRUE) ~ "CDD_long", #CDD de plus de 6 mois
                type_contrat == "CDD" & (duree_contrat < 180) ~ "CDD_court", # CDD de moins de 6 mois
                type_contrat == "CDI" ~ "CDI",
                type_contrat == "interim" ~ "interim",
                type_contrat == "autre" ~ "autre",
                TRUE ~ "0")) %>% select(id_midas, emploi_bit),
            by = "id_midas") %>%
  dplyr::mutate(Emploi0623 = ifelse(emploi_bit == 1, 1,0)) %>% 
  dplyr::mutate(Emploi0623 = ifelse(is.na(Emploi0623), 0, Emploi0623)) %>% collect()
  #dplyr::mutate(Emploi0623 = ifelse(is.na(type_contratt),0,1)) 

# On classe les deux trajectoires selon l'inscription à FT en juin 2022
persistance_ft_rsa_non_de <- persistance_ft_rsa %>%filter(DE_x == 'Non DE')
persistance_ft_rsa_de <- persistance_ft_rsa %>%filter(DE_x == 'DE')
# et on regarde la situation en juin 2023 par rapport au RSA, au contrat et à France Travail en juin 2023
group <- rbind(cbind(table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 0),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 0),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 0),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 0),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 1),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 1),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 1),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 1)),
               cbind(table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 0)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 0)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 0)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 0)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 1)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 0 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 1)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==0 & persistance_ft_rsa$Emploi0623 == 1)%>% prop.table(),
                     table(persistance_ft_rsa$BRSA_12m == 1 & persistance_ft_rsa$de_12m ==1 & persistance_ft_rsa$Emploi0623 == 1)%>% prop.table())) %>% as.data.frame()
group <- group[c(2,4),]
names(group) <- c('Pas RSA, Pas FT, Pas Emploi',
                  'Pas RSA, FT, Pas Emploi',
                  'RSA, Pas FT, Pas Emploi',
                  'RSA, FT, Pas Emploi',
                  'Pas RSA, Pas FT, Emploi',
                  'Pas RSA, FT, Emploi',
                  'RSA, Pas FT, Emploi',
                  'RSA, FT, Emploi')

#Répartition de la situation des bénéficiaires du RSA de juin 2022 en juin 2023 (RSA / Inscription à France Travail / Emploi salarié)
write.xlsx(group, file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_croises.xlsx",
           sheet="croisement", append=TRUE)

# Création d'un graphique qui illustre la situation en juin 2022 (en emploi / sans emploi inscrit à FT / sans emploi et non inscrit à FT)
# Et les parts de passage à la situation en juin 2023 par rapport à l'emploi, l'inscription à FT et la perception du RSA
# Graphiques flow 
data <- persistance_ft_rsa %>% dplyr::select(id_midas, DE_x, Emploi0622, BRSA_12m, Emploi0623, de_12m) %>%
  mutate(Sit_0622 = case_when(DE_x == 'Non DE' & Emploi0622 == 1 ~ 'Non inscrit\nà France\nTravail, \nen emploi \n salarié',
                              DE_x == 'Non DE' & Emploi0622 == 0 ~ 'Non inscrit\nà France\nTravail,\nsans\nemploi\nsalarié',
                              DE_x == 'DE' & Emploi0622 == 1 ~ 'Inscrit\nà France\nTravail, \nen emploi \n salarié',
                              DE_x == 'DE' & Emploi0622 == 0 ~ 'Inscrit\nà France\nTravail,\nsans\nemploi\nsalarié'),
         
         Sit_0622_A = case_when(Emploi0622 == 1 ~ 'En emploi\nsalarié \n',
                                DE_x == 'Non DE' & Emploi0622 == 0 ~ 'Sans emploi salarié,\nnon inscrit à\nFrance Travail\n',
                                DE_x == 'DE' & Emploi0622 == 0 ~ 'Sans emploi salarié,\n inscrit à\nFrance Travail\n'),
         
         Sit_0623 = case_when(BRSA_12m == 0 & Emploi0623 == 0 & de_12m == 0 ~ 'Sans emploi salarié,\nsans RSA,\nnon inscrit à France\nTravail ',
                              BRSA_12m == 0 & Emploi0623 == 0 & de_12m == 1 ~ 'Sans emploi salarié,\nsans RSA, inscrit\n à France Travail ',
                              BRSA_12m == 0 & Emploi0623 == 1  ~ 'En emploi salarié,\nsans RSA\n',
                              BRSA_12m == 1 & Emploi0623 == 0 ~ 'Sans emploi salarié,\nau RSA\n',
                              BRSA_12m == 1 & Emploi0623 == 1  ~ 'En emploi salarié,\nau RSA '))

part_sit_0622 <- data %>% group_by(Sit_0622) %>%
  summarise(part_sit_0622 = round(n()/nrow(data)*100))

part_sit_0622_A <- data %>% group_by(Sit_0622_A) %>%
  summarise(part_sit_0622_A = round(n()/nrow(data)*100))

part_sit_0623 <- data %>% group_by(Sit_0623) %>%
  summarise(part_sit_0623 = round(n()/nrow(data)*100))

data_graph <- data %>%
  left_join(part_sit_0622) %>%
  left_join(part_sit_0622_A) %>%
  left_join(part_sit_0623) %>%
  dplyr::mutate(Sit_0622 = paste0(Sit_0622, "(", part_sit_0622, " %)"),
                Sit_0622_A = paste0(Sit_0622_A, "(", part_sit_0622_A, " %)"), 
                Sit_0623 = paste0(Sit_0623, "(", part_sit_0623, " %)"))

# Tables situation en 2023 (RSA / France Travail / Emploi salarié) en fonction de la situation en 2022
table <- cbind(table(data$Sit_0622_A,data$Sit_0623)) %>% as.data.frame()
write.xlsx(table, file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_croises.xlsx",
           sheet="A", append=TRUE)
table2 <- cbind(table(data$Sit_0622_A,data$Sit_0623) %>% prop.table(margin = 1)) %>% as.data.frame()
write.xlsx(table2, file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_croises.xlsx",
           sheet="A pct", append=TRUE)

#Création du graphique de passage
#Passage entre la situation en juin 22 et juin 23
library(ggalluvial)
dataset <- table(data_graph$Sit_0622_A,data_graph$Sit_0623) %>% as.data.frame() 

g <- ggplot(data = dataset,
       aes(axis1 = Var1, axis2 = Var2, y = Freq)) +
  geom_alluvium(aes(fill = Var1), linewidth = 1, curve_type = 'cubic') +
  geom_stratum(alpha = 1) +
  geom_text(stat = "stratum",
            aes(label = after_stat(stratum)), size = 2.5) +
  scale_x_discrete(limits = c("Situation en juin 22","Situation en juin 23"),
                   expand = c(0.25,0.25)) +
  theme_minimal() +
  theme(legend.position = "none")
g
ggsave(g,file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_bit_croises.jpg")
ggsave(g,file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_bit_croises.svg")
ggsave(g,file="C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/DF3_bit_croises.pdf")




##################### fin#########################
