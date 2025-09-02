# Copyright (C). 2025

# Auteures: Poppée Mongruel, Clara Ponton. DARES

# Ce programme informatique a été développé par la DARES.
# Il permet de produire les illustrations et les résultats de la publication N°54 de la collection Dares FOCUS "Bénéficiaires du RSA et inscription à France Travail : profil et situation"

# Le texte et les tableaux peuvent être consultés sur le site de la Dares : https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail-profil-et-situation

# Ce programme utilise les données Midas de juin 2022. 
# Dans les données utilisées, les bénéficiaires du RSA sont repérés par leur identifiant MIDAS, qui n'est diffusé que via le CASD

# Bien qu'ils n'existent aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler à la DARES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils y rencontreraient.
# Contact : contact.dares.dmq@travail.gouv.fr

# Ce programme a été exécuté pour la dernière fois le 02/09/2025 avec la version 4.5.1 de R.

rm(list=ls())


#### Import des librairies 
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
mois <- "2022-06-01" # on s'intéresse aux BRSA en juin 2022
mois_m <- paste0(substr(mois,1,4),substr(mois,6,7))

#### Creation des dates de debut et fin de mois 
debut_fin_mois <- data.frame(debut_mois = seq.Date(as.Date("2010/01/01"),
                                                   as.Date("2030/01/01"),
                                                   by = "+1 months")) %>%
  mutate(fin_mois = (debut_mois + months(1)) -1,
         mois = paste0(substr(debut_mois,1,4),substr(debut_mois,6,7)),
         mois_suivant = lead(mois),
         mois_precedent = lag(mois))

fin_mois_m <- pull(debut_fin_mois %>% filter(mois == mois_m) %>%
                   select(fin_mois))
debut_mois_m <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% 
                     select(debut_mois))

#### Spark ####
conf <- spark_config()
conf$`sparklyr.cores.local` <- 10  
conf$`sparklyr.shell.driver-memory` <- "150G"  # memoire allouee a l'unique executeur en local
conf$spark.storage.memoryFraction <- "0.1"    # fraction de la memoire allouee au sockage
conf$spark.sql.shuffle.partitions <- "200"     
conf$spark.driver.maxResultSize <- 0           

### Lancer la connexion Spark
sc <- spark_connect(master="local",config=conf)


# import de la base
de_ms <- spark_read_parquet(sc, memory=FALSE, "file:///C:/Users/Public/Documents/etudes/DF BRSA et DE/BRSA et DE/de_ms.parquet") %>% collect()


### Graphique 1 : Catégorie d'inscription et parcours d'accompagnement des bénéficiaires du RSA, ------------
#par rapport à l'ensemble des personnes inscrites à France Travail fin juin 2022


### Panorama sur les BRSA qui sont inscrits à france Travail (DEFM) en juin 2022, selon l'âge
defm_0622 <- rbind(de_ms %>%
  group_by(AGE) %>%
  summarise(
    OBS = n(),
    nb_DEFM = sum(DEFM, na.rm=TRUE),
    part_DEFM = sum(DEFM, na.rm=TRUE)*100/n(),
    part_cat_A = sum(cat_A, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_cat_B = sum(cat_B, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_cat_C = sum(cat_C, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_cat_D = sum(cat_D, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_cat_E = sum(cat_E, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_creat_ent = sum(createur_ent, na.rm=TRUE)*100/sum(DEFM, na.rm=TRUE),
    part_creat_ent_cat_E = sum(createur_ent, na.rm=TRUE)*100/sum(cat_E, na.rm=TRUE),
    part_contrat_aide_cat_E = sum(contrat_aide, na.rm=TRUE)*100/sum(cat_E, na.rm=TRUE)
  ),
  data.frame(
    AGE = "ensemble",
    OBS = n_distinct(de_ms$id_midas),
    nb_DEFM = sum(de_ms$DEFM, na.rm = TRUE),
    part_DEFM = sum(de_ms$DEFM, na.rm = TRUE)*100/n_distinct(de_ms$id_midas),
    part_cat_A = sum(de_ms$cat_A, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_cat_B = sum(de_ms$cat_B, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_cat_C = sum(de_ms$cat_C, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_cat_D = sum(de_ms$cat_D, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_cat_E = sum(de_ms$cat_E, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_creat_ent = sum(de_ms$createur_ent, na.rm=TRUE)*100/sum(de_ms$DEFM, na.rm = TRUE),
    part_creat_ent_cat_E = sum(de_ms$createur_ent, na.rm=TRUE)*100/sum(de_ms$cat_E, na.rm = TRUE),
    part_contrat_aide_cat_E = sum(de_ms$contrat_aide, na.rm=TRUE)*100/sum(de_ms$cat_E, na.rm = TRUE)
  )) 

# exporter en excel
write.xlsx(defm_0622, file="DF1.xlsx", 
           sheet="BRSA_DEFM_0622", append=TRUE)


# fonction pour obtenir la répartition des parcours d'accompagnement à France Travail
stat_parcours <- function(data, mois, horizon){
  
  mois_m <- paste0(substr(ymd(as.Date(mois))+months(horizon),1,4),
                   substr(ymd(as.Date(mois))+months(horizon),6,7))
  fin_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>%
                     select(fin_mois))
  debut_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% 
                       select(debut_mois))
    
  parcours <- data %>%  
    left_join(spark_read_parquet(sc, paste0(dir_spark, "/FHS/parcours.parquet"),
                                 memory=FALSE) %>% 
                select(id_midas, jourdv, jourfv, parcours) %>% 
                mutate(jourdv = as.Date(jourdv),
                              jourfv = as.Date(jourfv)) %>%
                filter(jourdv <= fin_mois  & ( is.na(jourfv) | jourfv >= debut_mois) ) %>% 
                # si un individu a plusieurs parcours, on priorise le parcours le plus intensif
                mutate(accompagnement = case_when(parcours == "GLO" ~ 4, # parcours global
                                                         parcours == "REN" ~ 3, # parcours renforcé
                                                         parcours == "GUI" ~ 2, # parcours guidé
                                                         parcours == "SUI" ~ 1, # parcours suivi
                                                         TRUE ~ 0),
                              duree_accompagnement = as.integer(jourfv - jourdv)) %>% 
                window_order(id_midas, desc(accompagnement)) %>% 
                group_by(id_midas) %>% 
                filter(row_number() == 1) %>% 
                ungroup() %>%
                collect(), 
              by="id_midas") %>%
    mutate(global = ifelse(parcours == "GLO",1,0),
                  renforce = ifelse(parcours == "REN",1,0),
                  suivi = ifelse(parcours == "SUI",1,0),
                  guide = ifelse(parcours == "GUI",1,0),
                  autre = ifelse(!parcours %in% c("GLO", "REN", "GUI", "SUI", NA),1,0),
                  accompagnement = ifelse(!is.na(parcours),1,0))
  
  nb_accompagnement <- sum(parcours$accompagnement, na.rm=TRUE)
  
  table_parcours <- parcours %>%
    summarise(
      obs = n(),
      part_accompagne = sum(accompagnement, na.rm=TRUE)/n()*100,
      global = sum(global, na.rm=TRUE)/nb_accompagnement*100,
      renforce = sum(renforce, na.rm=TRUE)/nb_accompagnement*100,
      suivi = sum(suivi, na.rm=TRUE)/nb_accompagnement*100,
      guide = sum(guide, na.rm=TRUE)/nb_accompagnement*100,
      autre = sum(autre, na.rm=TRUE)/nb_accompagnement*100
    )
  
  return(table_parcours)
  
}


# parcours à FT sur le mois pour les BRSA inscrits à FT en juin 2022
stat_parcours_0622_brsa <- cbind(data.frame(population = "BRSA inscrits a FT"), 
  stat_parcours(data = de_ms %>% select(id_midas, DE) %>% filter(DE == "DE"), 
                                         mois = "2022-06-01", horizon=0) )

# parcours à FT de l'ensemble des DEFM de juin 2022
fin_mois_m = pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))

de_0622 <- spark_read_parquet(sc, memory=FALSE, paste0(dir_spark, "/FHS/de.parquet")) %>%
  select(id_midas, NDEM, DATINS, DATANN, ANCIEN, CATREGR) %>% 
  mutate(DATINS = as.Date(DATINS),
                DATANN = as.Date(DATANN)) %>%
  filter(DATINS <= fin_mois_m & (is.na(DATANN) | DATANN > fin_mois_m)) %>%
  # ne garder que les DE en fin de mois en juin 2022, ie ayant inscription en cours le 30/06
  mutate(MOIS = mois_m) %>%
  window_order(id_midas, DATINS) %>% 
  group_by(id_midas) %>% 
  filter(row_number() == 1) %>% # Dédoublonne : garde la date d'inscription la plus récente
  ungroup() %>%
  # left_join avec le FHS pour avoir les catégories DEFM :
  left_join(spark_read_parquet(sc,
                               paste0(dir_spark, "/FHS","/e0.parquet"),
                               memory=FALSE) %>%
              mutate(NBHEUR = as.numeric(NBHEUR)) %>% 
              filter(MOIS == mois_m) %>% 
              window_order(id_midas, MOIS, desc(NBHEUR)) %>% 
              group_by(id_midas) %>% 
              filter(row_number() == 1) %>% # Dédoublonne : garde le nombre d'heure le plus important
              ungroup(), 
            by = c("id_midas" = "id_midas", "NDEM" = "NDEM", "MOIS" = "MOIS")) %>% 
  mutate(catnouv = case_when(
    CATREGR %in% c("1","2","3") & (is.na(NBHEUR) == TRUE | NBHEUR == 0) ~ "A",
    CATREGR %in% c("1","2","3") & (NBHEUR > 0 & NBHEUR <= 78) ~ "B",
    CATREGR %in% c("1","2","3") & (NBHEUR > 78) ~ "C",
    CATREGR == "4" ~ "D",
    CATREGR == "5" ~ "E",
    TRUE ~ "NP")) %>%
  collect()

stat_parcours_0622_defm <- cbind(data.frame(population = "ensemble DEFM"), 
    stat_parcours(data = de_0622, mois = "2022-06-01", horizon=0) )


# parcours à FT sur le mois pour les BRSA inscrits à FT (uniquement en cat A, B, C) en juin 2022 
stat_parcours_0622_brsa_ABC <- cbind(data.frame(population = "BRSA inscrits a FT (cat ABC)"), 
                                 stat_parcours(data = de_ms %>% filter(DE == "DE" & catnouv %in% c("A", "B", "C")) %>%
                                                 select(id_midas, DE), 
                                               mois = "2022-06-01", horizon=0) )

# parcours à FT de l'ensemble des DEFM de juin 2022
fin_mois_m = pull(debut_fin_mois %>% filter(mois == mois_m) %>% select(fin_mois))

stat_parcours_0622_defm_ABC <- cbind(data.frame(population = "ensemble DEFM (cat ABC)"), 
                                 stat_parcours(data = de_0622 %>% filter(catnouv %in% c("A", "B", "C")), mois = "2022-06-01", horizon=0) )

# exporter en excel
write.xlsx(rbind(stat_parcours_0622_brsa, stat_parcours_0622_defm,
                 stat_parcours_0622_brsa_ABC, stat_parcours_0622_defm_ABC), file="DF1.xlsx", 
           sheet="parcours_defm_0622", append=TRUE)


  

### Tableau 1 : Profil des BRSA selon qu'ils sont DEFM ou non en juin 2022 -------
brsa_de_non_de <- rbind(
  de_ms %>%
  group_by(DE) %>%
  summarise(
    OBS_ind = n(),
    part_brsa = n()*100/n_distinct(de_ms$id_midas),
    OBS_menage = n_distinct(id_menage),
    FEMME = sum(FEMME, na.rm=TRUE)*100/n(),
    HOMME = sum(HOMME, na.rm=TRUE)*100/n(),
    SEXE_inconnu = sum(SEXE_inconnu, na.rm=TRUE)*100/n(),
    AGE_25 = sum(AGE_25, na.rm=TRUE)*100/n(),
    AGE_25_29 = sum(AGE_25_29, na.rm=TRUE)*100/n(),
    AGE_30_39 = sum(AGE_30_39, na.rm=TRUE)*100/n(),
    AGE_40_49 = sum(AGE_40_49, na.rm=TRUE)*100/n(),
    AGE_50_59 = sum(AGE_50_59, na.rm=TRUE)*100/n(),
    AGE_60 = sum(AGE_60, na.rm=TRUE)*100/n(),
    AGE_inconnu = sum(AGE_inconnu, na.rm=TRUE)*100/n(),
    MTRSAVER_MOY = mean(MTRSAVER),
    RESTRRSA_MOY = mean(RESTRRSA),
    ENFCHAR_0 = sum(ENFCHAR_0, na.rm=TRUE)*100/n(),
    ENFCHAR_1 = sum(ENFCHAR_1, na.rm=TRUE)*100/n(),
    ENFCHAR_2 = sum(ENFCHAR_2, na.rm=TRUE)*100/n(),
    ENFCHAR_3plus = sum(ENFCHAR_3plus, na.rm=TRUE)*100/n(),
    PPA = sum(PPAVERS, na.rm=TRUE)*100/n(),
    AAH = sum(AAHVERS, na.rm=TRUE)*100/n(),
    isole_senf = sum(isole_senf, na.rm=TRUE)*100/n(),
    isole_1enf = sum(isole_1enf, na.rm=TRUE)*100/n(),
    isole_2enf = sum(isole_2enf, na.rm=TRUE)*100/n(),
    parent_isole = sum(parent_isole, na.rm=TRUE)*100/n(),
    couple_senf = sum(couple_senf, na.rm=TRUE)*100/n(),
    couple_1enf = sum(couple_1enf, na.rm=TRUE)*100/n(),
    couple_2enf = sum(couple_2enf, na.rm=TRUE)*100/n()
  ),
  data.frame(
    DE = "ensemble",
    OBS_ind = n_distinct(de_ms$id_midas),
    part_brsa = n_distinct(de_ms$id_midas)*100/n_distinct(de_ms$id_midas),
    OBS_menage = n_distinct(de_ms$id_menage),
    FEMME = sum(de_ms$FEMME, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    HOMME = sum(de_ms$HOMME, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    SEXE_inconnu = sum(de_ms$SEXE_inconnu, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_25 = sum(de_ms$AGE_25, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_25_29 = sum(de_ms$AGE_25_29, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_30_39 = sum(de_ms$AGE_30_39, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_40_49 = sum(de_ms$AGE_40_49, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_50_59 = sum(de_ms$AGE_50_59, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_60 = sum(de_ms$AGE_60, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AGE_inconnu = sum(de_ms$AGE_inconnu, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    MTRSAVER_MOY = mean(de_ms$MTRSAVER),
    RESTRRSA_MOY = mean(de_ms$RESTRRSA),
    ENFCHAR_0 = sum(de_ms$ENFCHAR_0, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    ENFCHAR_1 = sum(de_ms$ENFCHAR_1, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    ENFCHAR_2 = sum(de_ms$ENFCHAR_2, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    ENFCHAR_3plus = sum(de_ms$ENFCHAR_3plus, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    PPA = sum(de_ms$PPAVERS, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    AAH = sum(de_ms$AAHVERS, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    isole_senf = sum(de_ms$isole_senf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    isole_1enf = sum(de_ms$isole_1enf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    isole_2enf = sum(de_ms$isole_2enf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    parent_isole = sum(de_ms$parent_isole, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    couple_senf = sum(de_ms$couple_senf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    couple_1enf = sum(de_ms$couple_1enf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas),
    couple_2enf = sum(de_ms$couple_2enf, na.rm=TRUE)*100/n_distinct(de_ms$id_midas)
  )) %>%
  t() %>% as.data.frame %>% row_to_names(row_number=1)

# exporter en excel
write.xlsx(brsa_de_non_de, file="DF1.xlsx", 
           sheet="profil_brsa_defm", append=TRUE)


### Part d'emploi salarié des BRSA en juin 2022 selon qu'ils sont DE ou non DE 

# Mouvements de main d'oeuvre en 2022
mmo_22 <- spark_read_parquet(sc, memory=FALSE,
                             paste0(dir_spark, "/MMO/mmo_2022.parquet")) %>%
  select(id_midas, DebutCTT, FinCTT, Nature, ModeExercice, 
                L_Contrat_SQN, Salaire_Base, Quali_Salaire_Base) %>%
  filter(is.na(L_Contrat_SQN) == FALSE | id_midas != "" | 
                  is.na(id_midas) == FALSE) %>%
  filter(Quali_Salaire_Base == "7")

# créer des listes de bRSA pour chaque mois à partir de juillet 2022
for (i in c(0,1,3,6)){
  
  mois_cnaf <- paste0(substr(ymd(as.Date(mois))+months(i),6,7), substr(ymd(as.Date(mois))+months(i),3,4))
  
  name <- paste0('brsa_',mois_cnaf)
  
  assign(name , spark_read_parquet(sc, memory=FALSE,
                                   paste0(dir_spark, "/Allstat/minsoc/cnaf_minsoc_", mois_cnaf, ".parquet")) %>%
           select(id_midas, RSAVERS, MTRSAVER) %>%
           filter(RSAVERS %in% c("RSA droit commun", "RSA droit local",
                                        "RSA jeune", "RSA expérimental", "C", "L", "J", "E")) %>%
           filter(MTRSAVER > 0) %>%
           # sélectionner uniquement les BRSA qui perçoivent une allocation non nulle
           window_order(id_midas) %>%
           group_by(id_midas) %>%
           filter(row_number() == 1) %>% # Dédoublonne
           ungroup() %>%
           select(id_midas) %>% collect())
  
}

### Fonction pour avoir les stats sur les contrats
stat_contrats <- function(data=de_ms, mois, horizon){
  
  # mois d'intérêt
  mois_m <- paste0(substr(ymd(as.Date(mois))+months(horizon),1,4),
                   substr(ymd(as.Date(mois))+months(horizon),6,7))
  fin_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>%
                     select(fin_mois))
  debut_mois <- pull(debut_fin_mois %>% filter(mois == mois_m) %>% 
                       select(debut_mois))
  
  # deux mois auparavant (début du trimestre)
  mois_m2 <- paste0(substr(ymd(as.Date(mois))-months(2),1,4),
                   substr(ymd(as.Date(mois))-months(2),6,7))
  
  debut_mois_m2 <- pull(debut_fin_mois %>% filter(mois == mois_m2) %>% 
                       select(debut_mois))
  
  # récupérer les emplois en cours en fin de mois
  mmo <- mmo_22 %>% 
      filter(DebutCTT <= fin_mois & 
               (FinCTT > fin_mois | is.na(FinCTT) == TRUE | FinCTT == "")) %>%
      filter(Quali_Salaire_Base == "7")
  
  contrat <- data %>%
    select(id_midas, DE) %>%
    merge(
      y = mmo %>% 
        mutate(
          DebutCTT = to_date(DebutCTT),
          FinCTT = to_date(FinCTT),
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
          ),
          duree_contrat = case_when(
            is.na(FinCTT) == TRUE | 
              FinCTT == "" ~ as.integer(to_date("2030-01-01") - DebutCTT)+1,
            is.na(FinCTT) == FALSE ~ as.integer(FinCTT - DebutCTT)+1),
          temps_partiel = ifelse(ModeExercice %in% 
                                   c("20", "30", "41", "42", "40"), 1, 0), 
          temps_complet = ifelse(ModeExercice == "10", 1, 0)) %>% 
        add_count(id_midas) %>%
        rename(nb_contrat_ind = n) %>%
        dbplyr::window_order(id_midas, ordre_contrat, desc(duree_contrat)) %>%
        group_by(id_midas) %>% 
        filter(row_number() == 1) %>% # garder un contrat par ind en priorisant les CDI, puis CDD..
        ungroup %>% 
        mutate(contrat_encours = case_when(
          type_contrat == "CDD" & (duree_contrat >= 180 | is.na(duree_contrat) == TRUE) ~ "CDD_long", #CDD de plus de 6 mois
          type_contrat == "CDD" & (duree_contrat < 180) ~ "CDD_court", # CDD de moins de 6 mois
          type_contrat == "CDI" ~ "CDI",
          type_contrat == "interim" ~ "interim",
          type_contrat == "autre" ~ "autre",
          TRUE ~ "0")) %>%
        collect(),
      by = "id_midas",
      all.x = TRUE )
  
  
  stat_contrat <- contrat %>%
    mutate(
      type_contrat = paste0(type, " ", as.character(horizon), " mois"),
      contrat = ifelse(is.na(contrat_encours) | 
                                 contrat_encours ==0, 0, 1),
      emploi_durable = ifelse(contrat_encours %in% c("CDI", "CDD_long"),  1,  0),
      emploi_non_durable = ifelse(contrat_encours %in% c("CDD_court", "interim", "autre"),1,0),
      temps_partiel = ifelse(temps_partiel == 1, 1, 0),
      temps_complet = ifelse(temps_complet == 1, 1, 0),
      CDI = ifelse(contrat_encours == "CDI", 1, 0),
      CDD_long = ifelse(contrat_encours == "CDD_long", 1, 0),
      CDD_court = ifelse(contrat_encours == "CDD_court", 1, 0),
      interim = ifelse(contrat_encours == "interim", 1, 0),
      autre = ifelse(contrat_encours == "autre", 1, 0),
      CDI_partiel = ifelse(contrat_encours == "CDI" & temps_partiel == 1,1,0),
      CDI_temps_plein = ifelse(contrat_encours == "CDI" & temps_partiel == 0,1,0),
      debut_contrat_trimestre = ifelse(DebutCTT >= debut_mois_m2, 1,0)
    )
    
    stat_desc <- rbind(
      stat_contrat %>% group_by(DE) %>%
        summarise(
          type_contrat = unique(type_contrat),
          OBS_ind = n(),
          nb_emploi = sum(contrat, na.rm=TRUE),
          taux_emploi = sum(contrat, na.rm=TRUE)*100/n(),
          nb_CDI = sum(CDI, na.rm=TRUE),
          nb_CDD_long = sum(CDD_long, na.rm=TRUE),
          nb_CDD_court = sum(CDD_court, na.rm=TRUE),
          nb_interim = sum(interim, na.rm=TRUE),
          nb_autre = sum(autre, na.rm=TRUE), 
          part_CDI = sum(CDI, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_CDD_long = sum(CDD_long, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_CDD_court = sum(CDD_court, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_interim = sum(interim, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_autre = sum(autre, na.rm=TRUE)/sum(contrat, na.rm = TRUE)*100,
          part_emploi_durable = sum(emploi_durable, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_emploi_non_durable = sum(emploi_non_durable, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_temps_complet = sum(temps_complet, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_temps_partiel = sum(temps_partiel, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_CDI_partiel = sum(CDI_partiel, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          part_CDI_temps_plein = sum(CDI_temps_plein, na.rm=TRUE)/sum(contrat, na.rm=TRUE)*100,
          nb_CDI_temps_plein = sum(CDI_temps_plein, na.rm=TRUE),
          nb_debut_contrat_trim = sum(debut_contrat_trimestre, na.rm=TRUE),
          part_debut_contrat_trim = sum(debut_contrat_trimestre, na.rm=TRUE)/sum(contrat, na.rm=TRUE)
        ),
      data.frame(
        DE = "ensemble",
        type_contrat = unique(stat_contrat$type_contrat),
        OBS_ind = n_distinct(stat_contrat$id_midas),
        nb_emploi = sum(stat_contrat$contrat, na.rm=TRUE),
        taux_emploi = sum(stat_contrat$contrat, na.rm=TRUE)*100/n_distinct(stat_contrat$id_midas),
        nb_CDI = sum(stat_contrat$CDI, na.rm=TRUE),
        nb_CDD_long = sum(stat_contrat$CDD_long, na.rm=TRUE),
        nb_CDD_court = sum(stat_contrat$CDD_court, na.rm=TRUE),
        nb_interim = sum(stat_contrat$interim, na.rm=TRUE),
        nb_autre = sum(stat_contrat$autre, na.rm=TRUE),
        part_CDI = sum(stat_contrat$CDI, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_CDD_long = sum(stat_contrat$CDD_long, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_CDD_court = sum(stat_contrat$CDD_court, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_interim = sum(stat_contrat$interim, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_autre = sum(stat_contrat$autre, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_emploi_durable = sum(stat_contrat$emploi_durable, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_emploi_non_durable = sum(stat_contrat$emploi_non_durable, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_temps_complet = sum(stat_contrat$temps_complet, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_temps_partiel = sum(stat_contrat$temps_partiel, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_CDI_partiel = sum(stat_contrat$CDI_partiel, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        part_CDI_temps_plein = sum(stat_contrat$CDI_temps_plein, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)*100,
        nb_CDI_temps_plein = sum(stat_contrat$CDI_temps_plein, na.rm=TRUE),
        nb_debut_contrat_trim = sum(stat_contrat$debut_contrat_trimestre, na.rm=TRUE),
        part_debut_contrat_trim = sum(stat_contrat$debut_contrat_trimestre, na.rm=TRUE)/sum(stat_contrat$contrat, na.rm=TRUE)
      )
    )
    
    stat_desc <- cbind(stat_desc, 
                       data.frame(contrat_recent_sortie_RSA_1m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==1) %>% 
                                                           filter(!id_midas %in% brsa_0722$id_midas)),
                                                  nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==1) %>% 
                                                         filter(!id_midas %in% brsa_0722$id_midas)),
                                                  nrow(stat_contrat %>% filter(debut_contrat_trimestre ==1) %>%
                                                         filter(!id_midas %in% brsa_0722$id_midas)) ),
                                  contrat_recent_sortie_RSA_3m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==1) %>% 
                                                           filter(!id_midas %in% brsa_0922$id_midas)),
                                                    nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==1) %>% 
                                                           filter(!id_midas %in% brsa_0922$id_midas)),
                                                    nrow(stat_contrat %>% filter(debut_contrat_trimestre ==1) %>%
                                                           filter(!id_midas %in% brsa_0922$id_midas)) ),
                                  contrat_recent_sortie_RSA_6m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==1) %>% 
                                                           filter(!id_midas %in% brsa_1222$id_midas)),
                                                    nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==1) %>% 
                                                           filter(!id_midas %in% brsa_1222$id_midas)),
                                                    nrow(stat_contrat %>% filter(debut_contrat_trimestre ==1) %>%
                                                           filter(!id_midas %in% brsa_1222$id_midas)) ),
                                  contrat_durable_sortie_RSA_1m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_0722$id_midas)),
                                                    nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_0722$id_midas)),
                                                    nrow(stat_contrat %>% filter(debut_contrat_trimestre ==0) %>%
                                                            filter(!id_midas %in% brsa_0722$id_midas)) ),
                                  contrat_durable_sortie_RSA_3m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_0922$id_midas)),
                                                    nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_0922$id_midas)),
                                                    nrow(stat_contrat %>% filter(debut_contrat_trimestre ==0) %>%
                                                            filter(!id_midas %in% brsa_0922$id_midas)) ),
                                  contrat_durable_sortie_RSA_6m = c(nrow(stat_contrat %>% filter(DE == "DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_1222$id_midas)),
                                                    nrow(stat_contrat %>% filter(DE == "Non DE" & debut_contrat_trimestre ==0) %>% 
                                                            filter(!id_midas %in% brsa_1222$id_midas)),
                                                    nrow(stat_contrat %>% filter(debut_contrat_trimestre ==0) %>%
                                                            filter(!id_midas %in% brsa_1222$id_midas))  )                                                  )
                                  ) 
    
  return(stat_desc)
}



### Emploi salarié en juin 2022, selon situation familiale 

# Emploi salarié en fin de mois pour l'ensemble des BRSA de juin 22
contrat_fin_mois_m <- cbind(data.frame(population = rep(c("ensemble BRSA juin 22"),each=3) ),
                            stat_contrats(data = de_ms, mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les couples
contrat_fin_mois_m_couple <- cbind(data.frame(population = rep(c("BRSA en couple juin 22"),each=3) ),
                                   stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("3", "Couple")), 
                                                 mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les couples sans enfants
contrat_fin_mois_m_couple_senf <- cbind(data.frame(population = rep(c("BRSA en couple sans enfant juin 22"),each=3) ),
                                        stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("3", "Couple") & RSENAUTC == 0), 
                                                      mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les couples avec enfants
contrat_fin_mois_m_couple_enf <- cbind(data.frame(population = rep(c("BRSA en couple avec enfants juin 22"),each=3) ),
                                       stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("3", "Couple") & RSENAUTC > 0), 
                                                     mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les personnes isolées
contrat_fin_mois_m_isole <- cbind(data.frame(population = rep(c("BRSA célibataires juin 22"),each=3) ),
                                  stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("1", "2", "Isolé homme", "Isolé femme")),
                                                mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les personnes isolées sans enfant
contrat_fin_mois_m_isole_senf <- cbind(data.frame(population = rep(c("BRSA célibataires sans enfant juin 22"),each=3) ),
                                       stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("1", "2", "Isolé homme", "Isolé femme") & RSENAUTC == 0),
                                                     mois = mois, horizon = 0, type = "fin mois"))

# Emploi salarié en fin de mois pour les personnes isolées avec enfants
contrat_fin_mois_m_isole_enf <- cbind(data.frame(population = rep(c("BRSA célibataires avec enfants juin 22"),each=3) ),
                                      stat_contrats(data = de_ms %>% dplyr::filter(RSSITFAM %in% c("1", "2", "Isolé homme", "Isolé femme") &  RSENAUTC > 0),
                                                    mois = mois, horizon = 0, type = "fin mois"))

# exporter en excel
write.xlsx(rbind(contrat_fin_mois_m, contrat_fin_mois_m_couple, contrat_fin_mois_m_couple_senf, contrat_fin_mois_m_couple_enf,
                 contrat_fin_mois_m_isole, contrat_fin_mois_m_isole_senf, contrat_fin_mois_m_isole_enf), 
           file="DF1.xlsx", sheet="contrat_fin_mois_0622", append=TRUE)




### Carte : Part des bénéficiaires du RSA inscrits à France Travail en juin 2022, par département -------------

carte <- de_ms %>% 
  mutate(departement = if_else(nchar(CODEPOSD) == 4,
                               paste0("0",substr(CODEPOSD, 1, 1)),
                               substr(CODEPOSD, 1 ,2)),
         defm = ifelse(catnouv == "V" | is.na(catnouv) == T, "Non DE","DE")) %>% 
  group_by(defm, departement) %>% 
  summarise(effectif = n_distinct(id_midas, na.rm = T)) %>% 
  pivot_wider(names_from = defm, 
              values_from = effectif) %>% 
  mutate_all(~replace(., is.na(.), 0)) %>%
  mutate(brsa = DE + `Non DE`,
         `Part :` = (DE / brsa)*100)

# exporter en excel : la carte est réalisée depuis excel par le service maquettage de la Dares
write.xlsx(carte, file="DF1.xlsx", sheet="carte", append=TRUE)



##################### fin#########################
