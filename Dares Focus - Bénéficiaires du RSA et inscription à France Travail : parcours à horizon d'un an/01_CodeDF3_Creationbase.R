# Copyright (C). 2025

# Auteures: Poppée Mongruel, Clara Ponton. DARES

# Ce programme informatique a été développé par la DARES.
# Il permet de produire les illustrations et les résultats de la publication N°*XX* de la collection Dares FOCUS "Bénéficiaires du RSA et inscription à France Travail : parcours à horizon d'un an"

# Le texte et les tableaux peuvent être consultés sur le site de la Dares : https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail

# Ce programme utilise les données Midas de juin 2022 à juin 2023. 
# Dans les données utilisées, les bénéficiaires du RSA sont repérés par leur identifiant MIDAS, qui n'est diffusé que via le CASD

# Bien qu'ils n'existent aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler à la DARES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils y rencontreraient.
# Contact : contact.dares.dmq@travail.gouv.fr

# Ce programme a été exécuté pour la dernière fois le 16/07/2025 avec la version 4.5.1 de R.

#############
rm(list=ls())

Sys.setenv(SPARK_HOME="C:/USERS/MIDARES_C_PONTON0/AppData/Local/spark/spark-3.3.2-bin-hadoop3")

#### Import des librairies ------
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
dir_exhaustif <- "C:/Users/Public/Documents/MiDAS_parquet/Vague 4"
dir_spark <- "file:///C:/Users/Public/Documents/MiDAS_parquet/Vague 4"

### Dates ------------
calendrier <- data.frame(debut_mois = seq.Date(as.Date("2010/01/01"),
                                               as.Date("2030/01/01"),
                                               by = "+1 months")) %>%
  mutate(fin_mois = (debut_mois + months(1)) -1,
         mois = paste0(substr(debut_mois,1,4),substr(debut_mois,6,7)))
mois <- "2022-06-01" # on s'intéresse aux BRSA en juin 2022
mois_m <- paste0(substr(mois,1,4),substr(mois,6,7))
mois_cnaf <- paste0(substr(ymd(as.Date(mois)),6,7), substr(ymd(as.Date(mois)),3,4))
fin_mois_m = pull(calendrier %>% filter(mois == mois_m) %>% select(fin_mois))


#### Spark ####
conf <- spark_config()
conf$`sparklyr.cores.local` <- 14  
conf$`sparklyr.shell.driver-memory` <- "150G"  # memoire allouee a l'unique executeur en local
#conf$spark.storage.memoryFraction <- "0.1"    # fraction de la mémoire allouée au sockage
conf$spark.sql.shuffle.partitions <- "200"     
conf$spark.driver.maxResultSize <- 0           

### Lancer la connexion Spark
sc <- spark_connect(master="local",config=conf)



### 1. Traitement des bases exhaustives  --------------

## FHS - Demandeurs d'emploi en fin de mois en juin 2022 (inscription en cours le 30/06) ---------
de <- spark_read_parquet(sc, memory=FALSE,
                         paste0(dir_spark, "/FHS/de.parquet")) %>%
  dplyr::select(id_midas, NDEM, SEXE, NATION, DATINS, DATANN, ANCIEN, AGE, 
                NENF, NIVFOR, RSQSTAT, SITMAT, CATREGR, SITPAR, SITPAR_A) %>% 
  dplyr::mutate(DATINS = as.Date(DATINS),
                DATANN = as.Date(DATANN)) %>%
  dplyr::filter(DATINS <= fin_mois_m & (is.na(DATANN) | DATANN > fin_mois_m)) %>%
  dplyr::mutate(MOIS = mois_m) %>%
  window_order(id_midas, DATINS) %>% 
  group_by(id_midas) %>% 
  filter(row_number() == 1) %>% # Dédoublonne : garde la date d'inscription la plus récente
  ungroup() %>%
  # récupérer les catégories DEFM dans le FHS :
  left_join(spark_read_parquet(sc,
                               paste0(dir_spark, "/FHS","/e0.parquet"),
                               memory=FALSE) %>%
              dplyr::mutate(NBHEUR = as.numeric(NBHEUR)) %>% 
              filter(MOIS == mois_m) %>% 
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
    TRUE ~ "NP"))


# variable TOP_CONJ pour identifier le conjoint dans le foyer social
table_topconj <- spark_read_parquet(sc, memory=FALSE,
                                    paste0(dir_spark, "/Allstat/TOPCONJ/cnaf_topconj_", mois_cnaf, ".parquet")) %>%
  dplyr::mutate(id_menage = paste0(NORDALLC,NUMCAF)) %>%
  window_order(id_midas) %>%
  group_by(id_midas) %>%
  filter(row_number() == 1) %>% 
  ungroup()


## Table Ménage (CNAF) : récupérer les informations du foyer social ---------
table_menage <- spark_read_parquet(sc, 
                                   paste0(dir_spark,"/Allstat/menage/cnaf_menage_", mois_cnaf, ".parquet"), 
                                   memory = FALSE) %>%
  dplyr::select(c("NUMCAF","DTREFFRE","NORDALLC","SEXE", "SITFAM", 
                  "DTNAIRES", "SEXECONJ", "DTNAICON", "MTAFVERS", "MTCFVERS", "MTAIDELO",
                  "MTBPAJEV", "MTPAJCAV", "MTPAJPRV", "PAJPCAMM",
                  "PAJPCDOM", "PAJPCSTM", "MTREVBRU", "MTREVACT", "MTCHOFOY", 
                  "MTRANSAF", "MTALIMFO", "MTALIMVE", "MTPALIFT", "MTIJSSFT", "MTFONPAT",
                  "MTACFOY1", "MTACFOY2", "MTACFOY3", "MTCHFOY1", "MTCHFOY2",
                  "MTCHFOY3", "MTOTFOY1",  "MTOTFOY2",  "MTOTFOY3","ACTCONJ")) %>%
  dplyr::mutate(id_menage = paste0(NORDALLC,NUMCAF)) %>%
  window_order(id_menage) %>%
  group_by(id_menage) %>%
  filter(row_number() == 1) %>%
  ungroup()


## Table minimas sociaux : récupérer les bénéficiaires du RSA de juin 2022 ----------
table_ms <- spark_read_parquet(sc, paste0(dir_spark, 
                                          "/Allstat/minsoc/cnaf_minsoc_", mois_cnaf, ".parquet"), memory=FALSE) %>%
  dplyr::filter(RSAVERS %in% c("RSA droit commun", "RSA droit local",
                               "RSA jeune", "RSA expérimental", "C", "L", "J", "E")) %>%
  dplyr::filter(MTRSAVER > 0) %>%
  # sélectionner uniquement les BRSA qui perçoivent une allocation non nulle
  dplyr::mutate(id_menage = paste0(NORDALLC,NUMCAF)) %>%
  window_order(id_midas) %>%
  group_by(id_midas) %>%
  filter(row_number() == 1) %>% 
  ungroup()

# apparier les tables prestations et ménage
ms <- table_ms %>%
  left_join(table_menage, by="id_menage", suffix=c("_ms","_men")) %>%
  left_join(table_topconj, by=c("id_midas"="id_midas", "id_menage"="id_menage"))


# apparier les bases minimas sociaux et demandeurs d'emploi
# création des variables socio-démographiques
de_ms <- ms %>%
  left_join(de, by="id_midas",  suffix=c("_ms","_de")) %>%
  dplyr::select(-c(AAHPERE, COMPLAAH, TRIMPPA, PANBENAU, PPASITFA, RSAMAJI)) %>%
  collect() %>%
  dplyr::mutate(DE = ifelse(!is.na(DATINS),"DE","Non DE"),
                PPAVERS = ifelse(PPAVERS %in% 
                                   c("Prime d'Activité non majorée 25 ans et +", "2",
                                     "Prime d'Activité majorée", "3",
                                     "Prime d'Activité non majorée 18_25 ans", "1"),1,0),
                AAHVERS = ifelse(AAHVERS %in% 
                                   c("AAH pour handicapé ne travaillant pas en milieu protégé",
                                     "AAH pour handicapé travaillant en ESAT",
                                     "AAH pour handicapé travaillant autre qu'en ESAT",
                                     "1", "2", "3"),1,0),
                COUPLE = ifelse(RSSITFAM %in% c("Couple", "3"), 1,0),
                ISOLE_FEMME = ifelse(RSSITFAM %in% c("Isolé femme", "2"), 1,0),
                ISOLE_HOMME = ifelse(RSSITFAM %in% c("Isolé homme", "1") ,1,0),
                SIT_FAM_INCONNUE = ifelse(RSSITFAM %in% c("Sans signification ou inconnue", "0"),1,0),
                SEXE = case_when(
                  TOP_CONJ == 1 ~ SEXECONJ,
                  TOP_CONJ == 0 ~ SEXE_ms
                ), # récupérer le sexe du conjoint avec topconj
                FEMME = ifelse(SEXE == 2,1,0),
                HOMME = ifelse(SEXE == 1,1,0),
                SEXE_inconnu = ifelse(SEXE == "", 1,0),
                AGE = ifelse(TOP_CONJ == 1,
                             trunc(time_length(interval(as.Date(DTNAICON), fin_mois_m), "years")),
                             trunc(time_length(interval(as.Date(DTNAIRES), fin_mois_m), "years"))),
                AGE_25 = ifelse(AGE < 25, 1,0),
                AGE_25_29 = ifelse(AGE >= 25 & AGE < 30, 1,0),
                AGE_30_39 = ifelse(AGE >= 30 & AGE < 40, 1,0),
                AGE_40_49 = ifelse(AGE >= 40 & AGE < 50, 1,0),
                AGE_50_59 = ifelse(AGE >= 50 & AGE < 60, 1,0),
                AGE_60 = ifelse(AGE >= 60, 1,0),
                AGE_inconnu = ifelse(is.na(AGE),1,0),
                RSENAUTC = as.numeric(RSENAUTC),
                ENFCHAR_0 = ifelse(RSENAUTC == 0,1,0),
                ENFCHAR_1 = ifelse(RSENAUTC == 1,1,0),
                ENFCHAR_2 = ifelse(RSENAUTC == 2,1,0),
                ENFCHAR_3plus = ifelse(RSENAUTC >= 3,1,0),
                RESTRRSA = as.numeric(RESTRRSA),
                LOCATAIRE = ifelse(RSOCCLOG %in% c("Locataire ou sous locataire", "LOC"),1,0),
                PROPRIETAIRE = ifelse(RSOCCLOG %in%
                                        c("Propriétaire avec charges de remboursement",
                                          "Propriétaire sans charges de remboursement",
                                          "PRO", "ACC"),1,0),
                HEBERGEMENT = ifelse(RSOCCLOG %in%
                                       c("Hébergement à titre gratuit par des particuliers",
                                         "Hébergement collectif à titre gratuit",
                                         "Hébergement collectif à titre onéreux",
                                         "Hébergement onéreux par des particuliers", 
                                         "HCG", "HCO", "HGP", "HOP"),1,0),
                SANS_RESID_STABLE = ifelse(RSOCCLOG %in% 
                                             c("Sans résid. stable avec forfait logement",
                                               "Sans résid. stable sans forfait logement",
                                               "SRG", "SRO"), 1,0),
                LOGEMENT_AUTRE = ifelse(RSOCCLOG %in% 
                                          c("Pas de droit ou code inconnu", "000",
                                            "Forfait logement à appliquer", "BAL",
                                            "Hôtel", "HOT", "OLI",
                                            "Occupation logement inconnue"),1,0)
  ) %>%
  dplyr::mutate(
    AGE = case_when(
      AGE_25 == 1 ~ "Moins de 25 ans",
      AGE_25_29 == 1 ~ "25-29 ans",
      AGE_30_39 == 1 ~ "30-39 ans",
      AGE_40_49 == 1 ~ "40-49 ans",
      AGE_50_59 == 1 ~ "50-59 ans",
      AGE_60 == 1 ~ "60 ans et plus",
      AGE_inconnu == 1 ~ 'age inconnu'),
    cat_A = ifelse(catnouv == "A",1,0),
    cat_B = ifelse(catnouv == "B",1,0),
    cat_C = ifelse(catnouv == "C",1,0),
    cat_D = ifelse(catnouv == "D",1,0),
    cat_E = ifelse(catnouv == "E",1,0),
    DEFM = ifelse(DE=="DE",1,0),
    createur_ent = ifelse(SITPAR_A == "CEN",1,0), 
    contrat_aide = ifelse(SITPAR_A %in% c("CUN", "CUM", "CIE", "CAE", "EAN", "CQU",
                                          "CPP", "CRE", "CAV", "CAD", "CES", "DDI"), 1,0), 
    RESTRRSA = ifelse(RESTRRSA == 99999, NA, RESTRRSA), 
    MTRSAVER = ifelse(MTRSAVER == 99999, NA, MTRSAVER), 
    isole_senf = ifelse(COUPLE == 0 & RSENAUTC == 0, 1, 0),
    isole_1enf = ifelse(COUPLE == 0 & RSENAUTC == 1, 1, 0),
    isole_2enf = ifelse(COUPLE == 0 & RSENAUTC > 1, 1, 0),
    parent_isole = ifelse(COUPLE == 0 & RSENAUTC > 0, 1, 0),
    couple_senf = ifelse(COUPLE == 1 & RSENAUTC == 0, 1, 0),
    couple_1enf = ifelse(COUPLE == 1 & RSENAUTC == 1, 1, 0),
    couple_2enf = ifelse(COUPLE == 1 & RSENAUTC > 1, 1, 0)) 



# Enregistrer la base
write_parquet(de_ms, sink="de_ms_DF3.parquet")
