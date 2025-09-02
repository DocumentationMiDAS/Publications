# Copyright (C). 2025

# Auteures: Poppée Mongruel, Clara Ponton. DARES

# Ce programme informatique a été développé par la DARES.
# Il permet de produire les illustrations et les résultats de la publication N°XX de la collection Dares FOCUS "Bénéficiaires du RSA et inscription à France Travail : parcours à horizon d'un an"

# Le texte et les tableaux peuvent être consultés sur le site de la Dares : https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail

# Ce programme utilise les données Midas de juin 2022 à juin 2023. 
# Dans les données utilisées, les bénéficiaires du RSA sont repérés par leur identifiant MIDAS, qui n'est diffusé que via le CASD

# Bien qu'ils n'existent aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler à la DARES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils y rencontreraient.

# Ce programme a été exécuté pour la dernière fois le 16/07/2025 avec la version 4.5.1 de R.

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
dir_export <- "C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE"

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


### 1. Retour à l'emploi sur un an des BRSA selon qu'ils sont inscrits ou non à France Travail ----------

# Chargement de la base des BRSA en juin 22
de_ms <- spark_read_parquet(sc, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms_DF3.parquet", memory=FALSE)

# Créer des listes de bRSA pour chaque mois à partir de juin 2022 jusqu'à juin 2023
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
           dplyr::select(id_midas)) %>% collect()
  
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

# base mouvements de main d'oeuvre 2022 et 2023 : récupérer l'emploi salarié
mmo_22 <- spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/MMO/mmo_2022.parquet")) %>%
  dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, CodeApet, DispPolitiquePublique,
                L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>%
  dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                  is.na(id_midas) == FALSE) %>%
  filter(Quali_Salaire_Base == "7") %>%
  left_join(spark_read_parquet(sc, memory=FALSE, 
                               paste0("file:/C:/Users/Public/Documents/MiDAS_parquet/Vague 5/MMO/mmo_2022_complement.parquet")), 
            by="L_Contrat_SQN")

mmo_23 <- spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/MMO/mmo_2023.parquet")) %>%
  dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, CodeApet, DispPolitiquePublique,
                L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>%
  dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                  is.na(id_midas) == FALSE) %>%
  filter(Quali_Salaire_Base == "7") %>%
  left_join(spark_read_parquet(sc, memory=FALSE, 
                               paste0("file:/C:/Users/Public/Documents/MiDAS_parquet/Vague 5/MMO/mmo_2023_complement.parquet")), 
            by="L_Contrat_SQN")

# On joint la base mmo 2022 et 2023, et on recode les codes NAF
mmo_2223 <- sdf_bind_rows(mmo_22, mmo_23) %>%
  window_order(id_midas, L_Contrat_SQN, desc(DebutCTT)) %>% # on garde une observation par ind et par contrat
  group_by(id_midas, L_Contrat_SQN) %>% 
  filter(row_number() == 1) %>%  ungroup() %>% 
  dplyr::mutate(naf = substr(CodeApet,1,2)) %>% # rajouter une variable secteur d'activite à partir du code Apet (NAF)
  dplyr::mutate(secteur = case_when(
    naf %in% c("01","02","03") ~ "agriculture",
    naf %in% c("5", "6", "7", "8",  "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
               "26", "27", "28", "29", "30", "31", "32", "33", "35", "36", "37", "38", "39") ~ "industrie",
    naf %in% c("41","42","43") ~ "construction",
    naf %in% c("45", "46", "47") ~ "commerce",
    naf %in% c("49","50","51", "52", "53") ~ "transport entreposage",
    naf %in% c("55", "56") ~ "hebergement restauration",
    naf %in% c("58", "59", "60", "61", "62", "63") ~ "infocom",
    naf %in% c("64", "65", "66") ~ "finance et assurance",
    naf %in% c("68") ~ "immobilier",
    naf %in% c("69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82") ~ "science et administration",
    naf %in% c("84", "85", "86", "87", "88") ~ "administration publique",
    naf %in% c("90", "91", "92", "93", "94", "95", "96", "97", "98", "99") ~ "autres services",
    TRUE ~ "") ) %>%
  dplyr::mutate(naf_ut = substr(codeapet_ut,1,2)) %>% # rajouter une variable secteur d'activite à partir du code Apet (NAF)
  dplyr::mutate(secteur_ut = case_when(
    naf_ut %in% c("01","02","03") ~ "agriculture",
    naf_ut %in% c("5", "6", "7", "8",  "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
                  "26", "27", "28", "29", "30", "31", "32", "33", "35", "36", "37", "38", "39") ~ "industrie",
    naf_ut %in% c("41","42","43") ~ "construction",
    naf_ut %in% c("45", "46", "47") ~ "commerce",
    naf_ut %in% c("49","50","51", "52", "53") ~ "transport entreposage",
    naf_ut %in% c("55", "56") ~ "hebergement restauration",
    naf_ut %in% c("58", "59", "60", "61", "62", "63") ~ "infocom",
    naf_ut %in% c("64", "65", "66") ~ "finance et assurance",
    naf_ut %in% c("68") ~ "immobilier",
    naf_ut %in% c("69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82") ~ "science et administration",
    naf_ut %in% c("84", "85", "86", "87", "88") ~ "administration publique",
    naf_ut %in% c("90", "91", "92", "93", "94", "95", "96", "97", "98", "99") ~ "autres services",
    TRUE ~ "") ) %>%
  # pour les personnes en interim, recuperer le secteur utilisateur
  dplyr::mutate(secteur = ifelse(secteur == "science et administration" & secteur_ut != "science et administration"
                                 & secteur_ut != "" & secteur_ut != " ", secteur_ut, secteur))

# Début et fin du mois de juin 2022
fin_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))
debut_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(debut_mois))

#Calcul Niveau de vie du foyer: MTREVBRU 
de_ms <- de_ms %>% mutate(MTREVBRU = ifelse(MTREVBRU == 9999999,0,MTREVBRU))

# base : bRSA sans emploi en juin 22
# On recode les variables socio-démographiques
BRSA_sans_emploi_0622 <- de_ms %>%
  dplyr::mutate(SEXE = case_when(SEXE == 1 ~ "homme", 
                                 SEXE == 2 ~ "femme", 
                                 TRUE ~ "inconnu"), 
                situation_fam = case_when(isole_senf == 1  ~ "personne isolee",
                                          parent_isole == 1 ~ "famille monop", 
                                          couple_1enf == 1 | couple_2enf == 1 ~ "couple avec enf", 
                                          couple_senf == 1 ~ "couple sans enf", 
                                          TRUE ~ "inconnue"),
                niv_vie =case_when(MTREVBRU == 0 ~ "0 €",
                                   MTREVBRU > 0 & MTREVBRU < 2400 ~ "0-2400 €",
                                   MTREVBRU >= 2400 & MTREVBRU < 7500 ~ "2400-7500 €",
                                   MTREVBRU >= 7500 ~ ">7500€ €")) %>%
  dplyr::select(id_midas, SEXE, situation_fam, AGE, DE,niv_vie) %>%
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
  dplyr::select(id_midas, SEXE, situation_fam, AGE, DE, niv_vie) %>% # ajout variable entrant sur le trimestre
  left_join(brsa_0122 %>% dplyr::mutate(m1 = 1), by="id_midas") %>%
  left_join(brsa_0222 %>% dplyr::mutate(m2 = 1), by="id_midas") %>%
  left_join(brsa_0322 %>% dplyr::mutate(m3 = 1), by="id_midas") %>%
  dplyr::mutate(entrant_trimestre = ifelse(is.na(m1) & is.na(m2) & is.na(m3), "entrant_trim", "stock")) # entrant sur le trimestre T2 : ne pas avoir ete brsa au T1 (janv, fev, mars)


### Retour en emploi BIT ------------------

# boucle sur les mois (jusqu'a m+12) et les populations d'interet (DE/nonDE et stock/flux, selon l'age, le sexe, la situation familiale)
BRSA_sans_emploi_0622 <- BRSA_sans_emploi_0622 %>% mutate(var = 'ensemble')
liste_groupe <- c("var","entrant_trimestre", "AGE", "SEXE", "situation_fam","niv_vie")

#On regarde à m+12
for(groupe in liste_groupe){
  
  data <- BRSA_sans_emploi_0622 %>% dplyr::select(id_midas, matches(groupe),DE) %>%
    rename(groupe = 2)
  
  print(colnames(data))
  
  for(horizon in 12){
    
    print(horizon)
    
    # mois avec l'horizon (m+1, m+2 ...)
    mois_m <- paste0(substr(ymd(as.Date(mois))+months(horizon),1,4),
                     substr(ymd(as.Date(mois))+months(horizon),6,7))
    mois_emploi_bit <- paste0("emploi_bit_", substr(mois_m,1,4),"_", substr(mois_m,5,6))
    annee_horizon <- substr(ymd(as.Date(mois))+months(horizon),1,4)
    fin_mois_h <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))
    
    
    if(annee_horizon == "2022"){
      mmo <- mmo_22
    }else{mmo <- mmo_23}
    
    print(colnames(data))
    
    emploi_bit <- data %>% 
      left_join(mmo %>% 
                  select(matches(mois_emploi_bit), id_midas, DebutCTT, FinCTT, Nature, ModeExercice, QuotiteContrat, QuotiteReference) %>%
                  rename(emploi_bit = 1) %>%
                  filter(emploi_bit == 1) %>% # ,ne garder que les emploi bit au mois m
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
                    temps_partiel = ifelse(ModeExercice %in%  c("20", "30", "41", "42", "40", "99"), 1, 0), 
                    temps_complet = ifelse(ModeExercice == "10", 1, 0)) %>%
                  dbplyr::window_order(id_midas, ordre_contrat, desc(duree_contrat)) %>%
                  dplyr::group_by(id_midas) %>% 
                  mutate(nb_temps_complet = sum(temps_complet)) %>% 
                  mutate(temps_complet = ifelse(nb_temps_complet > 0, 1, 0)) %>%  # etre a temps complet si au moins un des contrats est à temps complet
                  dplyr::filter(row_number() == 1) %>% # garder un contrat par ind en priorisant les CDI, puis CDD.. (pour observer les contrats en cours à fin mois)
                  ungroup %>% 
                  dplyr::mutate(contrat_encours = case_when(
                    type_contrat == "CDD" & (duree_contrat >= 180 | is.na(duree_contrat) == TRUE) ~ "CDD_long", #CDD de plus de 6 mois
                    type_contrat == "CDD" & (duree_contrat < 180) ~ "CDD_court", # CDD de moins de 6 mois
                    type_contrat == "CDI" ~ "CDI",
                    type_contrat == "interim" ~ "interim",
                    type_contrat == "autre" ~ "autre",
                    TRUE ~ "0")), by = "id_midas") %>%
  dplyr::mutate(emploi_durable = ifelse(contrat_encours %in% c("CDI", "CDD_long"),  1,  0)) %>%
      group_by(DE, groupe) %>%
      summarise(nb_brsa_se = n(), 
                nb_emploi_bit = sum(emploi_bit, na.rm = TRUE), 
                nb_emploi_durable = sum(emploi_durable, na.rm = TRUE), 
                nb_temps_complet = sum(temps_complet, na.rm=TRUE),
                nb_temps_partiel = sum(temps_partiel, na.rm=TRUE)
      ) %>% collect() %>%
      dplyr::bind_rows(summarise(., across(where(is.numeric), sum), across(where(is.character), ~ "ensemble")))
    # On rajoute les part de retour à l'emploi
    emploi_bit <- emploi_bit %>% 
      mutate(retour_emploi_bit = nb_emploi_bit / nb_brsa_se, # part de retour en emploi bit
                    retour_emploi_durable = nb_emploi_durable / nb_brsa_se, # part de retour en emploi bit durable (CDI ou CDD de plus de 6 mois)
                    re_temps_complet = nb_temps_complet / nb_emploi_bit, 
                    mois = mois_m) %>%
      relocate(mois, .before =1)
    
    assign(paste0("emploi_bit_", horizon, "m"), emploi_bit)
    
  }
  
  write.xlsx(rbind(emploi_bit_12m),
             file="DF3_emploi_bit.xlsx", sheet=paste0("re_bit_", groupe), append=TRUE)
}


########################## fin ###############################
