# Copyright (C). 2025

# Auteures: Poppée Mongruel, Clara Ponton. DARES

# Ce programme informatique a été développé par la DARES.
# Il permet de produire les illustrations et les résultats de la publication N°58 de la collection Dares FOCUS "Bénéficiaires du RSA et inscription à France Travail : parcours sur une année"

# Le texte et les tableaux peuvent être consultés sur le site de la Dares : https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail-profil-et-situation

# Ce programme utilise les données Midas de juin 2021 à juin 2022. 
# Dans les données utilisées, les bénéficiaires du RSA sont repérés par leur identifiant MIDAS, qui n'est diffusé que via le CASD

# Bien qu'ils n'existent aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler à la DARES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils y rencontreraient.
# Contact : contact.dares.dmq@travail.gouv.fr

# Ce programme a été exécuté pour la dernière fois le 03/09/2025 avec la version 4.5.1 de R.

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
dir_exhaustif <- "C:/Users/Public/Documents/MiDAS_parquet/Vague 3"
dir_spark <- "file:///C:/Users/Public/Documents/MiDAS_parquet/Vague 3"

### Dates ------------
mois <- "2022-06-01" # on s'intéresse aux BRSA en juin 2022
mois_m <- paste0(substr(mois,1,4),substr(mois,6,7))

#### Creation des dates de debut et fin de mois ####
debut_fin_mois <- data.frame(debut_mois = seq.Date(as.Date("2010/01/01"),
                                                   as.Date("2030/01/01"),
                                                   by = "+1 months")) %>%
  mutate(fin_mois = (debut_mois + months(1)) -1,
         mois = paste0(substr(debut_mois,1,4),substr(debut_mois,6,7)),
         mois_suivant = lead(mois),
         mois_precedent = lag(mois))

#### Spark ####
conf <- spark_config()
conf$`sparklyr.cores.local` <- 10  
conf$`sparklyr.shell.driver-memory` <- "150G"  # memoire allouee a l'unique executeur en local
#conf$spark.storage.memoryFraction <- "0.1"    # fraction de la memoire allou?e au sockage
conf$spark.sql.shuffle.partitions <- "200"     
conf$spark.driver.maxResultSize <- 0           

### Lancer la connexion Spark
sc <- spark_connect(master="local",config=conf)


# import de la base sur les BRSA inscrits a FT 
de_ms <- spark_read_parquet(sc, memory=FALSE, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms.parquet") %>% 
  dplyr::mutate(DE = paste0(DE, " juin 22")) %>%
  collect()


## 1. Chargement des données France Travail  ----------

fin_mois_1an <- pull(debut_fin_mois %>% filter(lead(mois, 12) == mois_m) %>%
                       select(fin_mois))
debut_mois_1an <- pull(debut_fin_mois %>% filter(lead(mois, 12) == mois_m) %>%
                         select(debut_mois))
mois_1an <- pull(debut_fin_mois %>% filter(lead(mois, 12) == mois_m) %>%
                   select(mois))
mois_cnaf_1an <- paste0(substr(mois_1an,5,6), substr(mois_1an,3,4))

debut_mois_m <- pull(debut_fin_mois %>% filter(mois == mois_m) %>%
                       select(debut_mois))

# Taille de l'étude: 1er juin 2021 -> 31 mai 2022 (1an pile)
fin_mois_m <- as.Date('2022-05-31') 


##### A. DEFM fin juin 2021 -----

###### a - Nombre et caractéristiques -----

# On créé une base pour pouvoir comptabiliser le nb de DEFM il y a 1 an (et leurs caractéristiques)
# join des bRSA à la date donnée avec la base des DEFM 1 an auparavant
defm_1an <- de_ms %>%
  select(id_midas, DE, DEFM) %>%
  left_join(spark_read_parquet(sc, memory=FALSE,
                               paste0(dir_spark, "/FHS/de.parquet")) %>%
              dplyr::select(id_midas, NDEM, SEXE, NATION, DATINS, DATANN, ANCIEN, AGE, 
                            NENF, NIVFOR, RSQSTAT, SITMAT, CATREGR, SITPAR_A) %>% 
              dplyr::mutate(DATINS = as.Date(DATINS),
                            DATANN = as.Date(DATANN)) %>%
              dplyr::filter(DATINS <= fin_mois_1an & (is.na(DATANN) | DATANN > fin_mois_1an)) %>%
              # ne garder que les DEFM juin 2021, ie ayant inscription en cours le 30/06/21
              dplyr::mutate(MOIS = mois_1an) %>%
              window_order(id_midas, DATINS) %>% 
              group_by(id_midas) %>% 
              filter(row_number() == 1) %>% # Dédoublonne : garde la date d'inscription la plus récente
              ungroup() %>%
              # left_join avec le FHS pour avoir les catégories DEFM :
              left_join(spark_read_parquet(sc,
                                           paste0(dir_spark, "/FHS","/e0.parquet"),
                                           memory=FALSE) %>%
                          dplyr::mutate(NBHEUR = as.numeric(NBHEUR)) %>%
                          filter(MOIS == mois_1an) %>% 
                          window_order(id_midas, MOIS, desc(NBHEUR)) %>% 
                          group_by(id_midas) %>% 
                          filter(row_number() == 1) %>% # Dédoublonne : garde le nombre d'heure le plus important
                          ungroup(), 
                        by = c("id_midas" = "id_midas", "NDEM" = "NDEM", "MOIS" = "MOIS")) %>% 
              dplyr::mutate(catnouv = case_when(
                CATREGR %in% c("1","2","3") & (is.na(NBHEUR) == TRUE | NBHEUR == 0) ~ "A",
                CATREGR %in% c("1","2","3") & (NBHEUR > 0 & NBHEUR <= 78) ~ "B",
                CATREGR %in% c("1","2","3") & (NBHEUR > 78) ~ "C",
                CATREGR == "4" ~ "D",
                CATREGR == "5" ~ "E",
                TRUE ~ "NP")) %>% 
              dplyr::mutate(
                cat_A = ifelse(catnouv == "A",1,0),
                cat_B = ifelse(catnouv == "B",1,0),
                cat_C = ifelse(catnouv == "C",1,0),
                cat_D = ifelse(catnouv == "D",1,0),
                cat_E = ifelse(catnouv == "E",1,0),
                createur_ent = ifelse(SITPAR_A == "CEN",1,0), 
                contrat_aide = ifelse(SITPAR_A %in% c("CUN", "CUM", "CIE", "CAE", "EAN", "CQU",
                                                      "CPP", "CRE", "CAV", "CAD", "CES", "DDI"), 1,0)
              ) %>%
              collect(),
            by="id_midas") %>%
  dplyr::mutate(DEFM_1an = ifelse(is.na(DATINS),0,1)) 


# stats sur les bRSA qui étaient inscrits à FT il y a un an
brsa_defm_1an <- rbind(
  defm_1an %>%
    dplyr::group_by(DE) %>%
    summarise(
      OBS = n(),
      nb_DEFM_1an = sum(DEFM_1an, na.rm = TRUE),
      part_DEFM_1an = sum(DEFM_1an, na.rm=TRUE)*100/n(),
      part_cat_A = sum(cat_A, na.rm=TRUE)*100/sum(DEFM_1an),
      part_cat_B = sum(cat_B, na.rm=TRUE)*100/sum(DEFM_1an),
      part_cat_C = sum(cat_C, na.rm=TRUE)*100/sum(DEFM_1an),
      part_cat_D = sum(cat_D, na.rm=TRUE)*100/sum(DEFM_1an),
      part_cat_E = sum(cat_E, na.rm=TRUE)*100/sum(DEFM_1an),
      part_creat_ent = sum(createur_ent, na.rm=TRUE)*100/sum(DEFM_1an, na.rm=TRUE),
      part_creat_ent_cat_E = sum(createur_ent, na.rm=TRUE)*100/sum(cat_E, na.rm=TRUE),
      part_contrat_aide_cat_E = sum(contrat_aide, na.rm=TRUE)*100/sum(cat_E, na.rm=TRUE)
    ),
  data.frame(
    DE = "ensemble",
    OBS = n_distinct(defm_1an$id_midas),
    nb_DEFM_1an = sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_DEFM_1an = sum(defm_1an$DEFM_1an, na.rm = TRUE)*100/n_distinct(defm_1an$id_midas),
    part_cat_A = sum(defm_1an$cat_A, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_cat_B = sum(defm_1an$cat_B, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_cat_C = sum(defm_1an$cat_C, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_cat_D = sum(defm_1an$cat_D, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_cat_E = sum(defm_1an$cat_E, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_creat_ent = sum(defm_1an$createur_ent, na.rm=TRUE)*100/sum(defm_1an$DEFM_1an, na.rm = TRUE),
    part_creat_ent_cat_E = sum(defm_1an$createur_ent, na.rm=TRUE)*100/sum(defm_1an$cat_E, na.rm = TRUE),
    part_contrat_aide_cat_E = sum(defm_1an$contrat_aide, na.rm=TRUE)*100/sum(defm_1an$cat_E, na.rm = TRUE)
  ))

# exporter en excel
# write.xlsx(brsa_defm_1an, file="DF2.xlsx",
#            sheet="BRSA_DEFM_0621", append=TRUE)


####### b - Parcours sur le mois pour les DEFM de juin 2021 ------
parcours_0621 <- de_ms %>% 
  dplyr::select(id_midas, DE) %>%
  inner_join(defm_1an %>% dplyr::select(id_midas, DEFM_1an), 
             by="id_midas")  %>%
  left_join(spark_read_parquet(sc, paste0(dir_spark, "/FHS/parcours.parquet"),
                               memory=FALSE) %>% 
              dplyr::select(id_midas, jourdv, jourfv, parcours) %>% 
              dplyr::mutate(jourdv = as.Date(jourdv),
                            jourfv = as.Date(jourfv)) %>%
              dplyr::filter(jourdv <= fin_mois_1an  & ( is.na(jourfv) | jourfv >= debut_mois_1an) ) %>% 
              dplyr::mutate(accompagnement = case_when(parcours == "GLO" ~ 4,
                                                       parcours == "REN" ~ 3,
                                                       parcours == "GUI" ~ 2,
                                                       parcours == "SUI" ~ 1,
                                                       TRUE ~ 0),
                            duree_accompagnement = as.integer(jourfv - jourdv)) %>% 
              window_order(id_midas, desc(accompagnement)) %>% 
              group_by(id_midas) %>% 
              filter(row_number() == 1) %>% 
              ungroup() %>%
              collect(), 
            by="id_midas") %>%
  dplyr::filter(DEFM_1an == 1) %>%
  dplyr::mutate(global = ifelse(parcours == "GLO",1,0),
                renforce = ifelse(parcours == "REN",1,0),
                suivi = ifelse(parcours == "SUI",1,0),
                guide = ifelse(parcours == "GUI",1,0),
                autre = ifelse(!parcours %in% c("GLO", "REN", "GUI", "SUI", NA),1,0),
                accompagnement = ifelse(!is.na(parcours),1,0))


# Type de parcours suivis en juin 21 par les BRSA de juin 22 qui étaient DEFM il y a un an
stat_parcours_0621 <- rbind(
  parcours_0621 %>%
    group_by(DE) %>%
    dplyr::summarise(
      defm_1an = n(),
      part_accompagne = sum(accompagnement, na.rm=TRUE)/n()*100,
      global = sum(global, na.rm=TRUE)/n()*100,
      renforce = sum(renforce, na.rm=TRUE)/n()*100,
      suivi = sum(suivi, na.rm=TRUE)/n()*100,
      guide = sum(guide, na.rm=TRUE)/n()*100,
      autre = sum(autre, na.rm=TRUE)/n()*100
    ),
  data.frame(
    DE = "ensemble des BRSA juin 22",
    defm_1an = nrow(parcours_0621),
    part_accompagne = sum(parcours_0621$accompagnement, na.rm=TRUE)/nrow(parcours_0621)*100,
    global = sum(parcours_0621$global, na.rm=TRUE)/nrow(parcours_0621)*100,
    renforce = sum(parcours_0621$renforce, na.rm=TRUE)/nrow(parcours_0621)*100,
    suivi = sum(parcours_0621$suivi, na.rm=TRUE)/nrow(parcours_0621)*100,
    guide = sum(parcours_0621$guide, na.rm=TRUE)/nrow(parcours_0621)*100,
    autre = sum(parcours_0621$autre, na.rm=TRUE)/nrow(parcours_0621)*100
  )
)

# # exporter en excel
# write.xlsx(stat_parcours_0621, file="DF2.xlsx",
#            sheet="Parcours DEFM0621", append=TRUE)


##### B. Au moins une fois DEFM en 1an -----

#On recharge la base
de_ms <- spark_read_parquet(sc, memory=FALSE, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms.parquet") %>% 
  dplyr::mutate(DE = paste0(DE, " juin 22"))

#On cherche à savoir combien de bRSA était inscrit au mois une fois à France Travail sur l'année passée 
inscription_ft_1an <- de_ms %>%
  select(id_midas, DE) %>%
  left_join(spark_read_parquet(sc, memory=FALSE,paste0(dir_spark, "/FHS/de.parquet")) %>%
              dplyr::select(id_midas, NDEM, DATINS, DATANN, ANCIEN), by = 'id_midas') %>%
  dplyr::mutate(DATINS = as.Date(DATINS),
                DATANN = as.Date(DATANN)) %>%
  dplyr::filter(DATINS <= as.Date("2022-05-31") & (is.na(DATANN) | DATANN > as.Date("2021-06-01"))) %>%
  dplyr::mutate(DATINS = case_when(DATINS < as.Date("2021-06-01") ~ as.Date("2021-06-01"),
                                   T ~ DATINS),
                DATANN = case_when(is.na(DATANN) | DATANN > as.Date("2022-05-31") ~ as.Date("2022-05-31"),
                                   T ~ DATANN))

#On réduit les overlaps de temps passé à FT
#Objectif : ne pas sommer deux périodes de temps d'inscription qui se superposerait
inscription_ft_1an <- inscription_ft_1an %>% 
  select(id_midas,DATINS,DATANN) %>% 
  arrange(id_midas,DATINS,DATANN) %>%
  group_by(id_midas) %>%
  mutate(deb_lag = lag(DATINS), fin_lag = lag(DATANN)) %>%
  ungroup()

inscription_ft_1an <- inscription_ft_1an %>% 
  mutate(is_pb = case_when(DATINS >= deb_lag & DATINS <= fin_lag ~ 0, T ~ 1))
inscription_ft_1an <- inscription_ft_1an %>% group_by(id_midas) %>% mutate(cs = cumsum(is_pb))

inscription_ft_1an <- inscription_ft_1an %>% 
  window_order(id_midas, DATINS) %>%
  group_by(id_midas,cs) %>%
  filter(row_number() == 1) %>%
  ungroup()
#Round 2 pour réduction des overlaps
inscription_ft_1an <- inscription_ft_1an %>% 
  select(id_midas,DATINS,DATANN) %>% 
  arrange(id_midas,desc(DATANN),DATINS) %>%
  group_by(id_midas) %>%
  mutate(deb_lag = lag(DATINS), fin_lag = lag(DATANN)) %>%
  ungroup()
inscription_ft_1an <- inscription_ft_1an %>% 
  mutate(is_pb = case_when(DATANN >= deb_lag & DATANN <= fin_lag ~ 0, T ~ 1))
inscription_ft_1an <- inscription_ft_1an %>% group_by(id_midas) %>% mutate(cs = cumsum(is_pb))
inscription_ft_1an <- inscription_ft_1an %>% 
  window_order(id_midas, DATINS) %>%
  group_by(id_midas,cs) %>%
  filter(row_number() == 1) %>%# Dédoublonne : garde la date d'inscription la plus récente quans il y a overlaps
  ungroup()

# Calcule durée du temps passé à France Travail
inscription_ft_1an <- inscription_ft_1an %>% mutate(DureeCTT = DATEDIFF(DATANN,DATINS))
inscription_ft_1an <- inscription_ft_1an %>% group_by(id_midas) %>% summarise(temps_ft = sum(DureeCTT))

# Création de catégorie de temps passé à France Travail 
ft_id <- inscription_ft_1an %>% select(id_midas, temps_ft) %>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() %>%
  mutate(is_ft = 1,
         mois1 = ifelse(temps_ft > 0 & temps_ft <= 30, 1,0),
         mois2 = ifelse(temps_ft > 30 & temps_ft <= 60, 1,0),
         mois3 = ifelse(temps_ft > 60 & temps_ft <= 90, 1,0),
         mois4 = ifelse(temps_ft > 90 & temps_ft <= 120, 1,0),
         mois5 = ifelse(temps_ft > 120 & temps_ft <= 150, 1,0),
         mois6 = ifelse(temps_ft > 150 & temps_ft <= 180, 1,0),
         mois7 = ifelse(temps_ft > 180 & temps_ft <= 210, 1,0),
         mois8 = ifelse(temps_ft > 210 & temps_ft <= 240, 1,0),
         mois9 = ifelse(temps_ft > 240 & temps_ft <= 270, 1,0),
         mois10 = ifelse(temps_ft > 270 & temps_ft <= 300, 1,0),
         mois11 = ifelse(temps_ft > 300 & temps_ft <= 330, 1,0),
         mois12 = ifelse(temps_ft > 330, 1,0))

summary(inscription_ft_1an %>% select(temps_ft) %>% collect(), na.rm = T)

# join avec ensemble de la base brsa juin 22
de_ms_ft <- de_ms %>% left_join(ft_id, by = 'id_midas')%>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() 

de_ms_ft <- de_ms_ft %>% select(id_midas,DE,temps_ft,is_ft,mois1,mois2,mois3,
                                mois4,mois5,mois6,mois7,mois8,mois9,mois10,mois11,mois12) %>% collect()

# statistiques sur le temps passé à France Travail sur l'année précédente
temps_passe_ft <- rbind (
  de_ms_ft %>%
    group_by(DE) %>%
    summarise(
      nb_pers_ft = sum(is_ft, na.rm = T),
      part_pers_ft = sum(is_ft, na.rm = T) / n()*100,      
      temps_moy_ft = mean(temps_ft, na.rm=TRUE),
      temps_med_ft = median(temps_ft, na.rm=TRUE),
      nb_ft_1mois = sum(mois1, na.rm = T),
      part_ft_1mois = sum(mois1, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_2mois = sum(mois2, na.rm = T),
      part_ft_2mois = sum(mois2, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_3mois = sum(mois3, na.rm = T),
      part_ft_3mois = sum(mois3, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_4mois = sum(mois4, na.rm = T),
      part_ft_4mois = sum(mois4, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_5mois = sum(mois5, na.rm = T),
      part_ft_5mois = sum(mois5, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_6mois = sum(mois6, na.rm = T),
      part_ft_6mois = sum(mois6, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_7mois = sum(mois7, na.rm = T),
      part_ft_7mois = sum(mois7, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_8mois = sum(mois8, na.rm = T),
      part_ft_8mois = sum(mois8, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_9mois = sum(mois9, na.rm = T),
      part_ft_9mois = sum(mois9, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_10mois = sum(mois10, na.rm = T),
      part_ft_10mois = sum(mois10, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_11mois = sum(mois11, na.rm = T),
      part_ft_11mois = sum(mois11, na.rm = T) / sum(is_ft, na.rm = T) * 100,
      nb_ft_12mois = sum(mois12, na.rm = T),
      part_ft_12mois = sum(mois12, na.rm = T) / sum(is_ft, na.rm = T) * 100
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    nb_pers_ft = sum(de_ms_ft$is_ft, na.rm = T),
    part_pers_ft = sum(de_ms_ft$is_ft, na.rm = T) / nrow(de_ms_ft)*100,      
    temps_moy_ft = mean(de_ms_ft$temps_ft, na.rm=TRUE),
    temps_med_ft = median(de_ms_ft$temps_ft, na.rm=TRUE),
    nb_ft_1mois = sum(de_ms_ft$mois1, na.rm = T),
    part_ft_1mois = sum(de_ms_ft$mois1, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_2mois = sum(de_ms_ft$mois2, na.rm = T),
    part_ft_2mois = sum(de_ms_ft$mois2, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_3mois = sum(de_ms_ft$mois3, na.rm = T),
    part_ft_3mois = sum(de_ms_ft$mois3, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_4mois = sum(de_ms_ft$mois4, na.rm = T),
    part_ft_4mois = sum(de_ms_ft$mois4, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_5mois = sum(de_ms_ft$mois5, na.rm = T),
    part_ft_5mois = sum(de_ms_ft$mois5, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_6mois = sum(de_ms_ft$mois6, na.rm = T),
    part_ft_6mois = sum(de_ms_ft$mois6, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_7mois = sum(de_ms_ft$mois7, na.rm = T),
    part_ft_7mois = sum(de_ms_ft$mois7, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_8mois = sum(de_ms_ft$mois8, na.rm = T),
    part_ft_8mois = sum(de_ms_ft$mois8, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_9mois = sum(de_ms_ft$mois9, na.rm = T),
    part_ft_9mois = sum(de_ms_ft$mois9, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_10mois = sum(de_ms_ft$mois10, na.rm = T),
    part_ft_10mois = sum(de_ms_ft$mois10, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_11mois = sum(de_ms_ft$mois11, na.rm = T),
    part_ft_11mois = sum(de_ms_ft$mois11, na.rm = T) /sum(de_ms_ft$is_ft, na.rm = T) * 100,
    nb_ft_12mois = sum(de_ms_ft$mois12, na.rm = T),
    part_ft_12mois = sum(de_ms_ft$mois12, na.rm = T) / sum(de_ms_ft$is_ft, na.rm = T) * 100
  )
)

# exporter en excel
# write.xlsx(temps_passe_ft, file="DF2.xlsx",
#            sheet="DEFM 1an ", append=TRUE)


#### Statistiques sur les formations et immersions des inscrits à France Travail en juin 21 ----
#formation immersion :
formation <- spark_read_parquet(sc, "file:///C:/Users/Public/Documents/MiDAS_parquet/Vague 4/FHS/p2.parquet",
                                memory = FALSE)  %>% select(id_midas, P2DATDEB, P2DATFIN) %>%
  # on ne garde que les entr?es en formation au mois m
  dplyr::filter(P2DATDEB <= fin_mois_m & P2DATDEB >= debut_mois_1an) %>%
  group_by(id_midas) %>%
  summarize(nb_formations = n()) %>% collect()

immersion <- spark_read_parquet(sc,
                                "file:///C:/Users/Public/Documents/MiDAS_parquet/Vague 4/SISP/immersions.parquet",
                                memory=FALSE)%>% select(id_midas, DATEDEB) %>%
  # on ne garde que les entr?es en immersion au mois m
  dplyr::filter(DATEDEB %in% c("202106","202107","202108","202109","202110","202111",
                               "202112","202201","202202","202203","202204","202205")) %>%
  group_by(id_midas) %>%
  summarize(nb_immersions = n()) %>% collect()


brsa_immfor <- de_ms_ft %>% 
  filter(is_ft == 1 ) %>%
  select(id_midas,DE) %>% 
  left_join(formation, by = 'id_midas') %>% 
  left_join(immersion, by = 'id_midas')
brsa_immfor <- brsa_immfor %>% mutate(nb_formations = ifelse(is.na(nb_formations), 0, nb_formations),
                                      nb_immersions = ifelse(is.na(nb_immersions), 0, nb_immersions))

distribution_formation <- as.data.frame(
  rbind(table(brsa_immfor$DE, brsa_immfor$nb_formations),
        table(brsa_immfor$nb_formations),
        table(brsa_immfor$DE, brsa_immfor$nb_formations) %>% prop.table(margin = 1),
        prop.table(table(brsa_immfor$nb_formations))) )

distribution_immersion <- as.data.frame(
  rbind(table(brsa_immfor$DE, brsa_immfor$nb_immersions),
        table(brsa_immfor$nb_immersions),
        table(brsa_immfor$DE, brsa_immfor$nb_immersions) %>% prop.table(margin = 1),
        prop.table(table(brsa_immfor$nb_immersions))) )

# # exporter en excel
# write.xlsx(distribution_formation, file="DF2.xlsx",
#            sheet="formation v2", append=TRUE)
# 
# # exporter en excel
# write.xlsx(distribution_immersion, file="DF2.xlsx",
#            sheet="inscription v2", append=TRUE)



#
##
#
##
#
##
#


### 2. RSA ----

##### liste des BRSA entre 0621 et 0522 -----
for (i in 1:12){
  mois_cnaf <- paste0(substr(ymd(as.Date(mois)) - months(i),6,7), substr(ymd(as.Date(mois)) - months(i),3,4))
  print(mois_cnaf)
  name <- paste0('brsa_',mois_cnaf)
  name_id <- paste0('brsa_',mois_cnaf)
  
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
           dplyr::select(id_midas) %>%
           mutate(BRSA = 1))
}

#join de la base des bRSA avec celles de tous les mois précédents
#pour connaître la présence au RSA au cours de l'année précédente
#et le nombre de mois passé au RSA sur 1 an en sommant les 12 mois
anciennete_rsa <- de_ms %>% 
  dplyr::select(id_midas, DE) %>% 
  left_join(brsa_0621, by = 'id_midas', suffix = c("","_12m")) %>% 
  left_join(brsa_0721, by = 'id_midas', suffix = c("","_11m")) %>%
  left_join(brsa_0821, by = 'id_midas', suffix = c("","_10m")) %>%
  left_join(brsa_0921, by = 'id_midas', suffix = c("","_9m")) %>%
  left_join(brsa_1021, by = 'id_midas', suffix = c("","_8m")) %>%
  left_join(brsa_1121, by = 'id_midas', suffix = c("","_7m")) %>%
  left_join(brsa_1221, by = 'id_midas', suffix = c("","_6m")) %>%
  left_join(brsa_0122, by = 'id_midas', suffix = c("","_5m")) %>%
  left_join(brsa_0222, by = 'id_midas', suffix = c("","_4m")) %>%
  left_join(brsa_0322, by = 'id_midas', suffix = c("","_3m")) %>%
  left_join(brsa_0422, by = 'id_midas', suffix = c("","_2m")) %>%
  left_join(brsa_0522, by = 'id_midas', suffix = c("","_1m")) %>%
  rename(BRSA_12m = BRSA) %>%
  mutate(BRSA_12m = ifelse(is.na(BRSA_12m), 0, 1),
         BRSA_11m = ifelse(is.na(BRSA_11m), 0, 1),
         BRSA_10m = ifelse(is.na(BRSA_10m), 0, 1),
         BRSA_9m = ifelse(is.na(BRSA_9m), 0, 1),
         BRSA_8m = ifelse(is.na(BRSA_8m), 0, 1),
         BRSA_7m = ifelse(is.na(BRSA_7m), 0, 1),
         BRSA_6m = ifelse(is.na(BRSA_6m), 0, 1),
         BRSA_5m = ifelse(is.na(BRSA_5m), 0, 1),
         BRSA_4m = ifelse(is.na(BRSA_4m), 0, 1),
         BRSA_3m = ifelse(is.na(BRSA_3m), 0, 1),
         BRSA_2m = ifelse(is.na(BRSA_2m), 0, 1),
         BRSA_1m = ifelse(is.na(BRSA_1m), 0, 1)) %>%
  mutate(mois_RSA_1an = BRSA_12m + BRSA_11m + BRSA_10m + BRSA_9m + BRSA_8m + 
           BRSA_7m + BRSA_6m + BRSA_5m + BRSA_4m + BRSA_3m + BRSA_2m + BRSA_1m)

anciennete_rsa_col <- anciennete_rsa %>% collect()

##### stat bRSA 0621 : savoir si le bRSA était au RSA 1 an auparavant -----
anciennete_rsa_0621 <- rbind(
  anciennete_rsa_col %>% 
    dplyr::group_by(DE) %>%
    dplyr::summarise(
      OBS = n(),
      nb_RSA_1an = sum(BRSA_12m, na.rm=TRUE),
      part_RSA_1an = sum(BRSA_12m, na.rm=TRUE)*100/n(),
      nb_mois_RSA_moy = mean(mois_RSA_1an, na.rm=TRUE)
    ),
  data.frame(
    DE = "ensemble",
    OBS = n_distinct(anciennete_rsa_col$id_midas),
    nb_RSA_1an = sum(anciennete_rsa_col$BRSA_12m, na.rm=TRUE),
    part_RSA_1an = sum(anciennete_rsa_col$BRSA_12m, na.rm=TRUE)*100/n_distinct(anciennete_rsa_col$id_midas),
    nb_mois_RSA_moy = mean(anciennete_rsa_col$mois_RSA_1an, na.rm=TRUE))
) 

# #exporter en excel
# write.xlsx(anciennete_rsa_0621, file="DF2.xlsx",
#            sheet="anciennete_rsa_0621", append=TRUE)


##### table de répartition du nombre de mois passés au RSA sur l'année précédente
distribution_rsa_1an <- as.data.frame(
  rbind(table(anciennete_rsa_col$DE, anciennete_rsa_col$mois_RSA_1an),
        table(anciennete_rsa_col$mois_RSA_1an),
        table(anciennete_rsa_col$DE, anciennete_rsa_col$mois_RSA_1an) %>% prop.table(margin = 1),
        prop.table(table(anciennete_rsa_col$mois_RSA_1an))) )

# #exporter en excel
# write.xlsx(distribution_rsa_1an, file="DF2.xlsx",
#            sheet="distribution_rsa_1an2", append=TRUE)


#
##
#
##
#
##
#


### 3. CONTRAT EMPLOI -----------

rm(de_ms)
#on retélécharge la base 
de_ms <- spark_read_parquet(sc, memory=FALSE, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms.parquet") %>% 
  dplyr::mutate(DE = paste0(DE, " juin 22"))

##### MMO : on charge l'ensemble des contrats salariés en 2022 et 2023 -----
mmo <- rbind(
  mmo_22 <- spark_read_parquet(sc, memory=FALSE,
                               paste0(dir_spark, "/MMO/mmo_2022.parquet")) %>%
    dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, 
                  L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base) %>%
    dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                    is.na(id_midas) == FALSE) %>%
    filter(Quali_Salaire_Base == "7"),
  mmo_21 <- spark_read_parquet(sc, memory=FALSE,
                               paste0(dir_spark, "/MMO/mmo_2021.parquet")) %>%
    dplyr::select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, 
                  L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base) %>%
    dplyr::filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                    is.na(id_midas) == FALSE) %>%
    filter(Quali_Salaire_Base == "7") ) %>%
  mutate(type_contrat = case_when(
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
  window_order(id_midas, L_Contrat_SQN, FinCTT) %>%
  group_by(id_midas, L_Contrat_SQN) %>%
  filter(row_number() == 1) %>% # Dédoublonne
  ungroup()


##### A. Signature d'un contrat -----

# Le contrat COMMENCE entre le 01 JUIN 2021 et le 31 MAI 2022   

###### a - Contrats ------
# on récupère les contrats qui commencent l'année précédente
contrats_1an_deb <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  left_join(mmo, by="id_midas") %>%
  dplyr::filter(!is.na(L_Contrat_SQN)) %>%
  dplyr::filter(DebutCTT <= fin_mois_m & DebutCTT >= as.Date('2021-06-01'))%>%
  dplyr::mutate(FinCTT = as.Date(FinCTT),
                DebutCTT = as.Date(DebutCTT)) %>%
  dplyr::mutate(FinCTT = case_when(is.na(FinCTT) ~ fin_mois_m,
                                   FinCTT > fin_mois_m ~ fin_mois_m,
                                   TRUE ~ FinCTT)) %>%
  dplyr::mutate(duree_contrat = as.integer(FinCTT - DebutCTT) + 1) %>% #+1 pour compter les contrats de 1 jours (qui commencent et finissent le même jour)
  group_by(id_midas) %>%
  dplyr::mutate(nb_contrat = n()) %>%
  dplyr::mutate(contrat_moins_1m = ifelse(duree_contrat < 30, 1, 0),
                contrat_1m_6m = ifelse(duree_contrat >= 30 & duree_contrat < 180, 1, 0),
                contrat_plus_6m = ifelse(duree_contrat >= 180, 1, 0)) %>%
  dplyr::mutate(CDI = ifelse(ordre_contrat == 1,1,0),
                CDD = ifelse(ordre_contrat == 2,1,0),
                interim = ifelse(ordre_contrat == 3,1,0),
                autre = ifelse(ordre_contrat == 4,1,0))

# stats sur les contrats sur un an (nb, moy)
contrats_1an_deb_col <- contrats_1an_deb %>% collect()
historique_contrat_1an_deb <- rbind (
  contrats_1an_deb_col %>%
    group_by(DE) %>%
    summarise(
      total_contrats = n(),
      duree_moy_contrat = mean(duree_contrat, na.rm=TRUE),
      part_contrat_moins_1m = sum(contrat_moins_1m, na.rm=TRUE)/n()*100,
      part_contrat_1m_6m = sum(contrat_1m_6m, na.rm=TRUE)/n()*100,
      part_contrat_plus_6m = sum(contrat_plus_6m, na.rm=TRUE)/n()*100
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    total_contrats = nrow(contrats_1an_deb_col),
    duree_moy_contrat = mean(contrats_1an_deb_col$duree_contrat, na.rm=TRUE),
    part_contrat_moins_1m = sum(contrats_1an_deb_col$contrat_moins_1m, na.rm=TRUE)/nrow(contrats_1an_deb_col)*100,
    part_contrat_1m_6m = sum(contrats_1an_deb_col$contrat_1m_6m, na.rm=TRUE)/nrow(contrats_1an_deb_col)*100,
    part_contrat_plus_6m = sum(contrats_1an_deb_col$contrat_plus_6m, na.rm=TRUE)/nrow(contrats_1an_deb_col)*100
  )
)

#exporter en excel
# write.xlsx(historique_contrat_1an_deb, file="DF2.xlsx",
#            sheet="deb contrat 1 an ", append=TRUE)


###### b - bRSA qui commencent un contrat --------

# on récupère les id de ceux qui ont un contrat qui débute dans la période
contrats_id_deb <- contrats_1an_deb %>% select(id_midas, Nature, ordre_contrat,nb_contrat) %>% 
  window_order(id_midas,ordre_contrat) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup()

# join avec l'ensemble des bRSA juin 22
de_ms_ct_deb <- de_ms %>% left_join(contrats_id_deb, by = 'id_midas')%>%
  dplyr::mutate(CDI = ifelse(ordre_contrat == 1,1,0),
                CDD = ifelse(ordre_contrat == 2,1,0),
                interim = ifelse(ordre_contrat == 3,1,0),
                autre = ifelse(ordre_contrat == 4,1,0),
                is_contrat = ifelse(is.na(Nature),0,1) )

ens_contrat_deb <- de_ms_ct_deb %>%
  select(id_midas,DE,CDI,CDD,interim,autre,is_contrat,nb_contrat) %>%
  collect()

#### stat sur les bRSA qui débutent un  contrat
deb_contrat_1an <- rbind(
  ens_contrat_deb %>%
    group_by(DE) %>%
    summarise(
      part_contrat = sum(is_contrat, na.rm=TRUE)/n()*100,
      part_CDI = sum(CDI, na.rm=TRUE)/n()*100,
      part_CDD = sum(CDD, na.rm=TRUE)/n()*100,
      part_interim = sum(interim, na.rm=TRUE)/n()*100,
      part_autre = sum(autre, na.rm=TRUE)/n()*100,
      nb_moy_contrats = mean(nb_contrat, na.rm=TRUE)
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    part_contrat = sum(ens_contrat_deb$is_contrat, na.rm=TRUE)/nrow(ens_contrat_deb)*100,
    part_CDI = sum(ens_contrat_deb$CDI, na.rm=TRUE)/nrow(ens_contrat_deb)*100,
    part_CDD = sum(ens_contrat_deb$CDD, na.rm=TRUE)/nrow(ens_contrat_deb)*100,
    part_interim = sum(ens_contrat_deb$interim, na.rm=TRUE)/nrow(ens_contrat_deb)*100,
    part_autre = sum(ens_contrat_deb$autre, na.rm=TRUE)/nrow(ens_contrat_deb)*100,
    nb_moy_contrats = mean(ens_contrat_deb$nb_contrat, na.rm=TRUE)
  )
) 

# write.xlsx(deb_contrat_1an, file="DF2.xlsx",
#            sheet="deb au moins un contrat 1 an ", append=TRUE)


##### B. Contrat en cours 01/06/21 ------------
### Le contrat est en cours le 1er juin 2021 

###### a - Contrats -----
# On récupère les contrats en cours 
contrats_1an_enc <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  left_join(mmo  , by="id_midas") %>%
  dplyr::filter(!is.na(L_Contrat_SQN))%>% 
  dplyr::filter((FinCTT >= as.Date('2021-06-01') | is.na(FinCTT) | FinCTT == '') &
                  DebutCTT < as.Date('2021-06-01')) %>%
  dplyr::mutate(FinCTT = as.Date(FinCTT),
                DebutCTT = as.Date(DebutCTT)) %>%
  dplyr::mutate(fin_contrat = ifelse(FinCTT <= fin_mois_m, 1,0)) %>%
  dplyr::mutate(FinCTT = case_when(is.na(FinCTT) ~ fin_mois_m,
                                   FinCTT >= fin_mois_m ~ fin_mois_m,
                                   TRUE ~ FinCTT),
                DebutCTT = case_when(DebutCTT < as.Date('2021-06-01') ~ as.Date('2021-06-01'),
                                     T ~ DebutCTT))   %>%
  dplyr::mutate(duree_contrat = as.integer(FinCTT - DebutCTT) + 1) %>% #+1 pour compter les contrats de 1 jours (qui commencent et finissent le même jour)
  group_by(id_midas) %>%
  dplyr::mutate(nb_contrat = n()) %>%
  dplyr::mutate(contrat_moins_1m = ifelse(duree_contrat < 30, 1, 0),
                contrat_1m_6m = ifelse(duree_contrat >= 30 & duree_contrat < 180, 1, 0),
                contrat_plus_6m = ifelse(duree_contrat >= 180, 1, 0)) %>%
  dplyr::mutate(CDI = ifelse(ordre_contrat == 1,1,0),
                CDD = ifelse(ordre_contrat == 2,1,0),
                interim = ifelse(ordre_contrat == 3,1,0),
                autre = ifelse(ordre_contrat == 4,1,0))

# stats sur les contrats en cours au 1er juin 21 (nb, moy)
contrats_1an_enc_col <- contrats_1an_enc %>% collect()
historique_contrat_1an_enc <- rbind (
  contrats_1an_enc_col %>%
    group_by(DE) %>%
    summarise(
      total_contrats = n(),
      duree_moy_contrat = mean(duree_contrat, na.rm=TRUE),
      dont_fin = sum(fin_contrat, na.rm=TRUE)/n()*100,
      part_contrat_moins_1m = sum(contrat_moins_1m, na.rm=TRUE)/n()*100,
      part_contrat_1m_6m = sum(contrat_1m_6m, na.rm=TRUE)/n()*100,
      part_contrat_plus_6m = sum(contrat_plus_6m, na.rm=TRUE)/n()*100
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    total_contrats = nrow(contrats_1an_enc_col),
    duree_moy_contrat = mean(contrats_1an_enc_col$duree_contrat, na.rm=TRUE),
    dont_fin = sum(contrats_1an_enc_col$fin_contrat, na.rm=TRUE)/nrow(contrats_1an_enc_col)*100,
    part_contrat_moins_1m = sum(contrats_1an_enc_col$contrat_moins_1m, na.rm=TRUE)/nrow(contrats_1an_enc_col)*100,
    part_contrat_1m_6m = sum(contrats_1an_enc_col$contrat_1m_6m, na.rm=TRUE)/nrow(contrats_1an_enc_col)*100,
    part_contrat_plus_6m = sum(contrats_1an_enc_col$contrat_plus_6m, na.rm=TRUE)/nrow(contrats_1an_enc_col)*100
  )
)

# # exporter en excel
# write.xlsx(historique_contrat_1an_enc, file="DF2.xlsx",
#            sheet="encours contrat 1 an ", append=TRUE)


###### b - bRSA qui ont un contrat en cours au 1er juin 21 -----

# on récupère les id des bRSA qui ont un contrat en cours au 1er juin 
contrats_id_enc <- contrats_1an_enc %>% select(id_midas, Nature, ordre_contrat,nb_contrat,fin_contrat) %>% window_order(id_midas,ordre_contrat) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() %>%
  mutate(is_contrat = 1)

# join avec l'ens des bRSA juin 22
de_ms_ct_enc <- de_ms %>% left_join(contrats_id_enc, by = 'id_midas')%>%
  dplyr::mutate(CDI = ifelse(ordre_contrat == 1,1,0),
                CDD = ifelse(ordre_contrat == 2,1,0),
                interim = ifelse(ordre_contrat == 3,1,0),
                autre = ifelse(ordre_contrat == 4,1,0))

ens_contrat_enc <- de_ms_ct_enc %>%
  select(id_midas,DE,CDI,CDD,interim,autre,is_contrat,nb_contrat,fin_contrat) %>%
  collect()

# statistiques 
is_contrat_1an_enc <- rbind(
  ens_contrat_enc %>%
    group_by(DE) %>%
    summarise(
      part_contrat = sum(is_contrat, na.rm=TRUE)/n()*100,
      dont_fin = sum(fin_contrat, na.rm=TRUE)/sum(is_contrat, na.rm=TRUE)*100,
      part_CDI = sum(CDI, na.rm=TRUE)/n()*100,
      part_CDD = sum(CDD, na.rm=TRUE)/n()*100,
      part_interim = sum(interim, na.rm=TRUE)/n()*100,
      part_autre = sum(autre, na.rm=TRUE)/n()*100,
      nb_moy_contrats = mean(nb_contrat, na.rm=TRUE)
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    part_contrat = sum(ens_contrat_enc$is_contrat, na.rm=TRUE)/nrow(ens_contrat_enc)*100,
    dont_fin = sum(ens_contrat_enc$fin_contrat, na.rm=TRUE)/sum(ens_contrat_enc$is_contrat, na.rm=TRUE)*100,
    part_CDI = sum(ens_contrat_enc$CDI, na.rm=TRUE)/nrow(ens_contrat_enc)*100,
    part_CDD = sum(ens_contrat_enc$CDD, na.rm=TRUE)/nrow(ens_contrat_enc)*100,
    part_interim = sum(ens_contrat_enc$interim, na.rm=TRUE)/nrow(ens_contrat_enc)*100,
    part_autre = sum(ens_contrat_enc$autre, na.rm=TRUE)/nrow(ens_contrat_enc)*100,
    nb_moy_contrats = mean(ens_contrat_enc$nb_contrat, na.rm=TRUE)
  )
) 


# #exporter en excel
# write.xlsx(is_contrat_1an_enc, file="DF2.xlsx",
#            sheet="encours au moins un contrat 1 an ", append=TRUE)


##### C. AU MOINS un contrat -------------
#Au moins un jour de contrat entre le 1er juin 21 et le 31 mai 2022

###### a - Tout type de contrat -----

# on récupère les contrats entre le 1/6/21 et le 31/5/22
contrats_1an <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  left_join(mmo  , by="id_midas")%>% 
  dplyr::filter((FinCTT >= as.Date('2021-06-01') | is.na(FinCTT) | FinCTT == '')
                & DebutCTT <= fin_mois_m) %>%
  dplyr::filter(!is.na(L_Contrat_SQN)) %>%
  dplyr::mutate(FinCTT = as.Date(FinCTT),
                DebutCTT = as.Date(DebutCTT)) %>%
  dplyr::mutate(FinCTT = case_when(is.na(FinCTT) ~ fin_mois_m,
                                   FinCTT >= fin_mois_m ~ fin_mois_m,
                                   TRUE ~ FinCTT),
                DebutCTT = case_when(DebutCTT < as.Date('2021-06-01') ~ as.Date('2021-06-01'),
                                     T ~ DebutCTT)) 

#on garde le contrat le plus long parmi ceux qui ont le même id_contrat
contrats_1an_nb <- contrats_1an %>% window_order(id_midas, DebutCTT, L_Contrat_SQN, desc(FinCTT)) %>%
  dplyr::group_by(id_midas, L_Contrat_SQN) %>%
  dplyr::filter(row_number() == 1) %>%
  mutate(duree_contrat = DATEDIFF(FinCTT,DebutCTT) +1) %>% select(duree_contrat,DE) %>% collect()

temps_1_contrat <- rbind (
  contrats_1an_nb %>%
    group_by(DE) %>%
    summarise(
      temps_moy_contrat = mean(duree_contrat, na.rm=TRUE),
      temps_med_contrat = median(duree_contrat, na.rm=TRUE),
      temps_min_contrat = min(duree_contrat, na.rm=TRUE),
      temps_max_contrat = max(duree_contrat, na.rm=TRUE)
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",   
    temps_moy_contrat = mean(contrats_1an_nb$duree_contrat, na.rm=TRUE),
    temps_med_contrat = median(contrats_1an_nb$duree_contrat, na.rm=TRUE),
    temps_min_contrat = min(contrats_1an_nb$duree_contrat, na.rm=TRUE),
    temps_max_contrat = max(contrats_1an_nb$duree_contrat, na.rm=TRUE)
  )
)

# exporter en excel
# write.xlsx(temps_1_contrat, file="DF2.xlsx", 
#            sheet="duree d'1 contrat", append=TRUE)


#on réduit les overlaps de contrat
contrats_1an <- contrats_1an %>% 
  select(id_midas,DebutCTT,FinCTT) %>% 
  arrange(id_midas,DebutCTT) %>%
  group_by(id_midas) %>%
  mutate(deb_lag = lag(DebutCTT), fin_lag = lag(FinCTT)) %>%
  ungroup()

contrats_1an <- contrats_1an %>% mutate(is_pb = case_when(DebutCTT >= deb_lag & DebutCTT <= fin_lag ~ 0, T ~ 1))
contrats_1an <- contrats_1an %>% group_by(id_midas) %>% mutate(cs = cumsum(is_pb))
contrats_1an <- contrats_1an %>% group_by(id_midas,cs) %>% summarise(DebutCTT = min(DebutCTT), FinCTT= max(FinCTT))

#calcule durée des contrats et temps de travail 
contrats_1an <- contrats_1an %>% mutate(DureeCTT = DATEDIFF(FinCTT,DebutCTT) +1)
#mmo_ante <- mmo_ante %>% select(id_midas, L_Contrat_SQN,DureeCTT)
contrats_1an <- contrats_1an %>% group_by(id_midas) %>% summarise(temps_en_contrat = sum(DureeCTT))

contrat_test <- contrats_1an %>% collect()

# on récupère les id et leur temps en contrat 
contrats_id <- contrats_1an %>% select(id_midas, temps_en_contrat) %>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() %>%
  mutate(is_contrat = 1,
         mois1 = ifelse(temps_en_contrat > 0 & temps_en_contrat <= 30, 1,0),
         mois2 = ifelse(temps_en_contrat > 30 & temps_en_contrat <= 60, 1,0),
         mois3 = ifelse(temps_en_contrat > 60 & temps_en_contrat <= 90, 1,0),
         mois4 = ifelse(temps_en_contrat > 90 & temps_en_contrat <= 120, 1,0),
         mois5 = ifelse(temps_en_contrat > 120 & temps_en_contrat <= 150, 1,0),
         mois6 = ifelse(temps_en_contrat > 150 & temps_en_contrat <= 180, 1,0),
         mois7 = ifelse(temps_en_contrat > 180 & temps_en_contrat <= 210, 1,0),
         mois8 = ifelse(temps_en_contrat > 210 & temps_en_contrat <= 240, 1,0),
         mois9 = ifelse(temps_en_contrat > 240 & temps_en_contrat <= 270, 1,0),
         mois10 = ifelse(temps_en_contrat > 270 & temps_en_contrat <= 300, 1,0),
         mois11 = ifelse(temps_en_contrat > 300 & temps_en_contrat <= 330, 1,0),
         mois12 = ifelse(temps_en_contrat > 330 & temps_en_contrat <= 365, 1,0))

# join avec l'ensemble des bRSA juin 22
de_ms_clt <- de_ms %>% left_join(contrats_id, by = 'id_midas')%>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() 

de_ms_clt <- de_ms_clt %>% select(id_midas,DE,temps_en_contrat,is_contrat,mois1,mois2,mois3,
                                  mois4,mois5,mois6,mois7,mois8,mois9,mois10,mois11,mois12) %>% collect()

# statistiques temps de contrat sur l'année
temps_passe_contrat <- rbind (
  de_ms_clt %>%
    group_by(DE) %>%
    summarise(
      nb_pers_contrat = sum(is_contrat, na.rm = T),
      part_pers_contrat = sum(is_contrat, na.rm = T) / n()*100,      
      temps_moy_contrat = mean(temps_en_contrat, na.rm=TRUE),
      temps_med_contrat = median(temps_en_contrat, na.rm=TRUE),
      nb_contrat_1mois = sum(mois1, na.rm = T),
      part_contrat_1mois = sum(mois1, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_2mois = sum(mois2, na.rm = T),
      part_contrat_2mois = sum(mois2, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_3mois = sum(mois3, na.rm = T),
      part_contrat_3mois = sum(mois3, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_4mois = sum(mois4, na.rm = T),
      part_contrat_4mois = sum(mois4, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_5mois = sum(mois5, na.rm = T),
      part_contrat_5mois = sum(mois5, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_6mois = sum(mois6, na.rm = T),
      part_contrat_6mois = sum(mois6, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_7mois = sum(mois7, na.rm = T),
      part_contrat_7mois = sum(mois7, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_8mois = sum(mois8, na.rm = T),
      part_contrat_8mois = sum(mois8, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_9mois = sum(mois9, na.rm = T),
      part_contrat_9mois = sum(mois9, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_10mois = sum(mois10, na.rm = T),
      part_contrat_10mois = sum(mois10, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_11mois = sum(mois11, na.rm = T),
      part_contrat_11mois = sum(mois11, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_12mois = sum(mois12, na.rm = T),
      part_contrat_12mois = sum(mois12, na.rm = T) / sum(is_contrat, na.rm = T) * 100
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    nb_pers_contrat = sum(de_ms_clt$is_contrat, na.rm = T),
    part_pers_contrat = sum(de_ms_clt$is_contrat, na.rm = T) / nrow(de_ms_clt)*100,      
    temps_moy_contrat = mean(de_ms_clt$temps_en_contrat, na.rm=TRUE),
    temps_med_contrat = median(de_ms_clt$temps_en_contrat, na.rm=TRUE),
    nb_contrat_1mois = sum(de_ms_clt$mois1, na.rm = T),
    part_contrat_1mois = sum(de_ms_clt$mois1, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_2mois = sum(de_ms_clt$mois2, na.rm = T),
    part_contrat_2mois = sum(de_ms_clt$mois2, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_3mois = sum(de_ms_clt$mois3, na.rm = T),
    part_contrat_3mois = sum(de_ms_clt$mois3, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_4mois = sum(de_ms_clt$mois4, na.rm = T),
    part_contrat_4mois = sum(de_ms_clt$mois4, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_5mois = sum(de_ms_clt$mois5, na.rm = T),
    part_contrat_5mois = sum(de_ms_clt$mois5, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_6mois = sum(de_ms_clt$mois6, na.rm = T),
    part_contrat_6mois = sum(de_ms_clt$mois6, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_7mois = sum(de_ms_clt$mois7, na.rm = T),
    part_contrat_7mois = sum(de_ms_clt$mois7, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_8mois = sum(de_ms_clt$mois8, na.rm = T),
    part_contrat_8mois = sum(de_ms_clt$mois8, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_9mois = sum(de_ms_clt$mois9, na.rm = T),
    part_contrat_9mois = sum(de_ms_clt$mois9, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_10mois = sum(de_ms_clt$mois10, na.rm = T),
    part_contrat_10mois = sum(de_ms_clt$mois10, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_11mois = sum(de_ms_clt$mois11, na.rm = T),
    part_contrat_11mois = sum(de_ms_clt$mois11, na.rm = T) /sum(de_ms_clt$is_contrat, na.rm = T) * 100,
    nb_contrat_12mois = sum(de_ms_clt$mois12, na.rm = T),
    part_contrat_12mois = sum(de_ms_clt$mois12, na.rm = T) / sum(de_ms_clt$is_contrat, na.rm = T) * 100
  )
)

# # exporter en excel
# write.xlsx(temps_passe_contrat, file="DF2.xlsx", 
#            sheet="temps enc ontrat ", append=TRUE)

###### b - CDI -----

# on récupère les contrats entre le 1/6/21 et le 31/5/22
contrats_1an_cdi <- de_ms %>%
  dplyr::select(id_midas, DE) %>%
  left_join(mmo  , by="id_midas")%>% 
  dplyr::filter((FinCTT >= as.Date('2021-06-01') | is.na(FinCTT) | FinCTT == '')
                & DebutCTT <= fin_mois_m) %>%
  dplyr::filter(!is.na(L_Contrat_SQN))%>%
  dplyr::filter(ordre_contrat == 1) %>%
  dplyr::mutate(FinCTT = as.Date(FinCTT),
                DebutCTT = as.Date(DebutCTT)) %>%
  dplyr::mutate(FinCTT = case_when(is.na(FinCTT) ~ fin_mois_m,
                                   FinCTT >= fin_mois_m ~ fin_mois_m,
                                   TRUE ~ FinCTT),
                DebutCTT = case_when(DebutCTT < as.Date('2021-06-01') ~ as.Date('2021-06-01'),
                                     T ~ DebutCTT)) 


#on réduit les overlaps de contrat
contrats_1an_cdi <- contrats_1an_cdi %>% 
  select(id_midas,DebutCTT,FinCTT) %>% 
  arrange(id_midas,DebutCTT) %>%
  group_by(id_midas) %>%
  mutate(deb_lag = lag(DebutCTT), fin_lag = lag(FinCTT)) %>%
  ungroup()

contrats_1an_cdi <- contrats_1an_cdi %>% mutate(is_pb = case_when(DebutCTT >= deb_lag & DebutCTT <= fin_lag ~ 0, T ~ 1))
contrats_1an_cdi <- contrats_1an_cdi %>% group_by(id_midas) %>% mutate(cs = cumsum(is_pb))
contrats_1an_cdi <- contrats_1an_cdi %>% group_by(id_midas,cs) %>% summarise(DebutCTT = min(DebutCTT), FinCTT= max(FinCTT))

#calcule durée des contrats et temps de travail 
contrats_1an_cdi <- contrats_1an_cdi %>% mutate(DureeCTT = DATEDIFF(FinCTT,DebutCTT) +1)
#mmo_ante <- mmo_ante %>% select(id_midas, L_Contrat_SQN,DureeCTT)
contrats_1an_cdi <- contrats_1an_cdi %>% group_by(id_midas) %>% summarise(temps_en_contrat = sum(DureeCTT))

# contrat_test <- contrats_1an_cdi %>% collect()

# on récupère les id et leur temps en contrat 
contrats_id_cdi <- contrats_1an_cdi %>% select(id_midas, temps_en_contrat) %>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() %>%
  mutate(is_contrat = 1,
         mois1 = ifelse(temps_en_contrat > 0 & temps_en_contrat <= 30, 1,0),
         mois2 = ifelse(temps_en_contrat > 30 & temps_en_contrat <= 60, 1,0),
         mois3 = ifelse(temps_en_contrat > 60 & temps_en_contrat <= 90, 1,0),
         mois4 = ifelse(temps_en_contrat > 90 & temps_en_contrat <= 120, 1,0),
         mois5 = ifelse(temps_en_contrat > 120 & temps_en_contrat <= 150, 1,0),
         mois6 = ifelse(temps_en_contrat > 150 & temps_en_contrat <= 180, 1,0),
         mois7 = ifelse(temps_en_contrat > 180 & temps_en_contrat <= 210, 1,0),
         mois8 = ifelse(temps_en_contrat > 210 & temps_en_contrat <= 240, 1,0),
         mois9 = ifelse(temps_en_contrat > 240 & temps_en_contrat <= 270, 1,0),
         mois10 = ifelse(temps_en_contrat > 270 & temps_en_contrat <= 300, 1,0),
         mois11 = ifelse(temps_en_contrat > 300 & temps_en_contrat <= 330, 1,0),
         mois12 = ifelse(temps_en_contrat > 330 & temps_en_contrat <= 365, 1,0))

# join avec l'ensemble des bRSA juin 22
de_ms_clt_cdi <- de_ms %>% left_join(contrats_id_cdi, by = 'id_midas')%>% window_order(id_midas) %>% 
  group_by(id_midas) %>% filter(row_number() == 1) %>% ungroup() 

de_ms_clt_cdi <- de_ms_clt_cdi %>% select(id_midas,DE,temps_en_contrat,is_contrat,mois1,mois2,mois3,
                                          mois4,mois5,mois6,mois7,mois8,mois9,mois10,mois11,mois12) %>% collect()

# statistiques temps de contrat sur l'année
temps_passe_contrat_cdi <- rbind (
  de_ms_clt_cdi %>%
    group_by(DE) %>%
    summarise(
      nb_pers_contrat_cdi = sum(is_contrat, na.rm = T),
      part_pers_contrat = sum(is_contrat, na.rm = T) / n()*100,      
      temps_moy_contrat = mean(temps_en_contrat, na.rm=TRUE),
      temps_med_contrat = median(temps_en_contrat, na.rm=TRUE),
      nb_contrat_1mois = sum(mois1, na.rm = T),
      part_contrat_1mois = sum(mois1, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_2mois = sum(mois2, na.rm = T),
      part_contrat_2mois = sum(mois2, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_3mois = sum(mois3, na.rm = T),
      part_contrat_3mois = sum(mois3, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_4mois = sum(mois4, na.rm = T),
      part_contrat_4mois = sum(mois4, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_5mois = sum(mois5, na.rm = T),
      part_contrat_5mois = sum(mois5, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_6mois = sum(mois6, na.rm = T),
      part_contrat_6mois = sum(mois6, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_7mois = sum(mois7, na.rm = T),
      part_contrat_7mois = sum(mois7, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_8mois = sum(mois8, na.rm = T),
      part_contrat_8mois = sum(mois8, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_9mois = sum(mois9, na.rm = T),
      part_contrat_9mois = sum(mois9, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_10mois = sum(mois10, na.rm = T),
      part_contrat_10mois = sum(mois10, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_11mois = sum(mois11, na.rm = T),
      part_contrat_11mois = sum(mois11, na.rm = T) / sum(is_contrat, na.rm = T) * 100,
      nb_contrat_12mois = sum(mois12, na.rm = T),
      part_contrat_12mois = sum(mois12, na.rm = T) / sum(is_contrat, na.rm = T) * 100
    ),
  data.frame(
    DE = "ensemble BRSA juin 22",
    nb_pers_contrat_cdi = sum(de_ms_clt_cdi$is_contrat, na.rm = T),
    part_pers_contrat = sum(de_ms_clt_cdi$is_contrat, na.rm = T) / nrow(de_ms_clt_cdi)*100,      
    temps_moy_contrat = mean(de_ms_clt_cdi$temps_en_contrat, na.rm=TRUE),
    temps_med_contrat = median(de_ms_clt_cdi$temps_en_contrat, na.rm=TRUE),
    nb_contrat_1mois = sum(de_ms_clt_cdi$mois1, na.rm = T),
    part_contrat_1mois = sum(de_ms_clt_cdi$mois1, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_2mois = sum(de_ms_clt_cdi$mois2, na.rm = T),
    part_contrat_2mois = sum(de_ms_clt_cdi$mois2, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_3mois = sum(de_ms_clt_cdi$mois3, na.rm = T),
    part_contrat_3mois = sum(de_ms_clt_cdi$mois3, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_4mois = sum(de_ms_clt_cdi$mois4, na.rm = T),
    part_contrat_4mois = sum(de_ms_clt_cdi$mois4, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_5mois = sum(de_ms_clt_cdi$mois5, na.rm = T),
    part_contrat_5mois = sum(de_ms_clt_cdi$mois5, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_6mois = sum(de_ms_clt_cdi$mois6, na.rm = T),
    part_contrat_6mois = sum(de_ms_clt_cdi$mois6, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_7mois = sum(de_ms_clt_cdi$mois7, na.rm = T),
    part_contrat_7mois = sum(de_ms_clt_cdi$mois7, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_8mois = sum(de_ms_clt_cdi$mois8, na.rm = T),
    part_contrat_8mois = sum(de_ms_clt_cdi$mois8, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_9mois = sum(de_ms_clt_cdi$mois9, na.rm = T),
    part_contrat_9mois = sum(de_ms_clt_cdi$mois9, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_10mois = sum(de_ms_clt_cdi$mois10, na.rm = T),
    part_contrat_10mois = sum(de_ms_clt_cdi$mois10, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_11mois = sum(de_ms_clt_cdi$mois11, na.rm = T),
    part_contrat_11mois = sum(de_ms_clt_cdi$mois11, na.rm = T) /sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100,
    nb_contrat_12mois = sum(de_ms_clt_cdi$mois12, na.rm = T),
    part_contrat_12mois = sum(de_ms_clt_cdi$mois12, na.rm = T) / sum(de_ms_clt_cdi$is_contrat, na.rm = T) * 100
  )
)

# # exporter en excel
# write.xlsx(temps_passe_contrat_cdi, file="DF2.xlsx", 
#            sheet="temps enc ontrat cdi ", append=TRUE)

