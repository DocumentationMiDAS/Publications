################################################################################
##################### CONSTRUCTION TABLEAU 1 ###################################
################################################################################

# J. DUCOULOMBIER, I. GLASER, L. FAUVRE
# 
# Dans ce programme on étudie les caractéristiques des allocataires en cours 
# d'indemnisation en décembre 2022, pour la colonne indemnisables fin 2022 du tableau 1

###Vider l'environnement de la session avant de commencer
rm(list=ls())

###Packages nécessaires
library(dplyr)
library(arrow)
library(haven)
library(lubridate)
library(reshape2)
library(duckdb)
library(dbplyr)
library(parquetize)
library(data.table)
library(ggplot2)
library(readr)
library(writexl)
library(tidyr)


### Bloc à modifier ####
date_deb <- as.Date("2022-07-01")
date_fin <- as.Date("2022-12-31")
mois <- "07_12"
annee <- "2022"



################################################################################
################## ENSEMBLE INDEMNISABLE CHAMP TEMPOREL ########################
################################################################################

### Chemin Dares
path_parquet <- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"


### Télécharger les tables (PJC à CDT)

nom_FNA <-list("pjc","odd", "dal", "cdt")

nom_parquet_FNA <- paste0(path_parquet, "/FNA/", nom_FNA, ".parquet") #définition des noms des tables FNA avec leur emplacement pour le chargement
nom_import_FNA <-list("pjc_p", "odd_p", "dal_p", "cdt_p") #nom que l'on veut donner aux tables FNA chargées dans R
for (i in 1 : length(nom_import_FNA)){
  assign(nom_import_FNA[[i]], 
         open_dataset(nom_parquet_FNA[i])
  ); gc()
}

indemnisable <- pjc_p %>% 
  select(id_midas, KROD3, KCPJC, KDDPJ, KDFPJ) %>% 
  filter(KDDPJ <= date_fin & KDFPJ >= date_deb) %>% 
  distinct(id_midas) %>%
  collect() 
id_indemnisable <- indemnisable$id_midas
rm(indemnisable)
gc()



### Récupérer les informations contenues dans la table DE

schema_de <- schema("id_midas" = string(), "SEXE" = string(), "NIVFOR" = string(), "DATINS"=date32(), "QUALIF" = string(), "datnais" = date32())

de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet), schema=schema_de)

de <- de_p %>% 
  filter(id_midas %in% id_indemnisable) %>% 
  collect()

de <- de %>% 
  group_by(id_midas) %>% 
  filter(DATINS == max(DATINS)) %>% 
  filter(!duplicated(id_midas)) # on récupère l'information la plus récente


### Regrouper les codes allocations

pjc_p <- pjc_p %>% 
  mutate(alloc = case_when(KCALF %in% c("01", "21", "22", "27", "40", "43", "47", "54", "64", "67", "82","AC", "BB", "BK", "BP", "CJ", "CN", "CQ", "DM", "EF", "EW", "FL", "GV", "HV")~ "ARE",
                           KCALF %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE")~ "AREF",
                           KCALF %in% c("AK", "AL", "AM", "AN", "AO","AP","AQ","AR","AS", "BU","BV","BW","BX", "BY","BZ","CA","CB","CC","CD","GC", "GD", "GE", "GG", "GH","GK", "GL",
                                        "FR", "FS", "FT", "FU", "FV", "FW", "FX")~ "ASP",
                           KCALF %in% c("EA", "GI", "EB", "GJ")~ "ATI",
                           KCALF %in% c("25", "30", "44", "51", "56","BD")~ "ASS",
                           TRUE ~ "Autres"))

odd_p <- odd_p %>% 
  mutate(alloc = case_when(KCAAJ %in% c("01", "21", "22", "27", "40", "43", "47", "54", "64", "67", "82","AC", "BB", "BK", "BP", "CJ", "CN", "CQ", "DM", "EF", "EW", "FL", "GV", "HV")~ "ARE",
                           KCAAJ %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE")~ "AREF",
                           KCAAJ %in% c("AK", "AL", "AM", "AN", "AO","AP","AQ","AR","AS", "BU","BV","BW","BX", "BY","BZ","CA","CB","CC","CD","GC", "GD", "GE", "GG", "GH","GK", "GL",
                                        "FR", "FS", "FT", "FU", "FV", "FW", "FX")~ "ASP",
                           KCAAJ %in% c("EA", "GI", "EB", "GJ")~ "ATI",
                           KCAAJ %in% c("25", "30", "44", "51", "56","BD")~ "ASS",
                           TRUE ~ "Autres"))


### Regrouper les motifs de fin de contrat

cdt <- cdt_p %>% 
  select(id_midas, "KCCT1"=KCCT, KDFPE, KCMCA) %>%
  mutate(KDFPE = as.Date(KDFPE)) %>%
  collect() %>%
  mutate(KCMCA_num = case_when(KCMCA =="XX" ~ 0,
                               KCMCA == ""~0,
                               TRUE ~ as.numeric(KCMCA))) %>% 
  mutate(passe_pro = case_when((KCMCA_num > 10 & KCMCA_num < 15) ~ "Licenciement_eco",
                               ((KCMCA_num >= 15 & KCMCA_num < 23) | (KCMCA_num > 27 & KCMCA_num < 30) | KCMCA_num == 25 | KCMCA_num == 38 | KCMCA_num == 50 | KCMCA_num == 57 | KCMCA_num == 91 | KCMCA_num == 95 | KCMCA_num == 80 | KCMCA_num == 85) ~ "Licenciement_autres",
                               KCMCA_num == 40 ~ "Fin_CDD",
                               KCMCA_num == 41 ~ "Fin_mission",
                               ((KCMCA_num > 59 & KCMCA_num < 80) | (KCMCA_num > 99 & KCMCA_num < 176) | KCMCA_num == 87| KCMCA_num == 45 | KCMCA_num == 24)~ "Depart_volontaire",
                               (KCMCA_num == 88 | KCMCA_num == 92) ~ "Rupture_conventionnelle",
                               KCMCA_num == 1~ "Manquant",
                               TRUE ~ "Autres")) 

odd <- odd_p %>% 
  select(id_midas, KROD1, KCRD, alloc, KPJDXP, KCCT1, KQCSJP) 

indemnisable <- pjc_p %>% 
  select(id_midas, KROD3, KCPJC, KDDPJ, KDFPJ) %>% 
  filter(KDDPJ <= date_fin & KDFPJ >= date_deb) %>% 
  distinct(id_midas, KROD3) %>%
  mutate(id_midas = as.character(id_midas),
         KROD3 = as.character(KROD3))

od <- pjc_p %>%
  group_by(id_midas, KROD3) %>%
  summarise(date_od = min(KDDPJ)) %>%
  ungroup %>%
  rename(KROD1 = KROD3) %>%
  mutate(id_midas = as.character(id_midas),
         KROD1 = as.character(KROD1))

regime <- indemnisable  %>% 
  rename(KROD1 = KROD3) %>%
  mutate(id_midas = as.character(id_midas),
         KROD1 = as.character(KROD1)) %>%
  inner_join(odd, by = c("id_midas", "KROD1")) %>% 
  inner_join(od, by = c("id_midas", "KROD1")) %>%
  filter(alloc %in% c("ARE", "AREF", "ASP")) %>%
  filter(KCRD != "28" & KCRD != "29") %>%
  rename(sjr_ouverture = KQCSJP,
         dpi_ouverture = KPJDXP) %>%
  distinct(id_midas, KROD1, sjr_ouverture, dpi_ouverture, KCCT1, date_od) %>%
  collect()

ajout_cdt <- regime  %>%
  left_join(cdt, by = c("id_midas", "KCCT1")) %>%
  select(-KCCT1) %>%
  collect()

data <- ajout_cdt %>%
  left_join(de, by = c("id_midas")) %>%
  distinct(id_midas, KROD1, .keep_all = T) %>% 
  mutate(KDFPE = as.Date(KDFPE),
         age_fc =  ifelse(!is.na(KDFPE) & !is.na(datnais), KDFPE - datnais, NA),
         age_fc = ifelse(is.na(age_fc), date_od - datnais, age_fc),
         age_fc = floor(age_fc/365)) %>%
  mutate(age_od = date_od - datnais,
         age_od = as.numeric(floor(age_od/365))) %>% 
  mutate(dpi_ouverture = 
           case_when(age_fc < 53 ~ ifelse(dpi_ouverture > 730, 730, dpi_ouverture),
                     age_fc >= 53 & age_fc < 55 ~ ifelse(dpi_ouverture > 913, 913, dpi_ouverture),
                     age_fc >= 55  ~ ifelse(dpi_ouverture > 1095, 1095, dpi_ouverture))) %>%
  filter(dpi_ouverture != 0)




#====================================#
### Caractéristiques individuelles ###
#====================================#


#### Distribution niveau de diplôme
data <- data %>%
  mutate(diplome_cat = case_when(NIVFOR=="" | is.na(NIVFOR) ~ "Inconnu", 
                                 NIVFOR %in% c("AFS", "CFG", "CP4", "C12", "C3A", "NV5") ~ "Inférieur au BAC-CAP-BEP",
                                 NIVFOR %in% c("NV4") ~"Bac",
                                 NIVFOR %in% c("NV3", "NV2", "NV1") ~"Bac +2 ou plus"))

distrib_nivfor <- data %>% 
  group_by(diplome_cat) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) 



#### Distribution de l'âge (à l'ouverture du droit)

age_interval <- c(0,24,52,100)
data <- data %>% 
  mutate(age_tranche = cut(age_od, breaks = age_interval))


distrib_age <- data %>% 
  group_by(age_tranche) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) 



#### Distribution sexe

distrib_sexe <- data %>% 
  group_by(SEXE) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) 



#### Distribution qualification

data <- data %>% 
  mutate(qualif_cat = case_when(QUALIF==0 | is.na(QUALIF) ~ "Inconnu",
                                QUALIF %in% c(1, 2, 5) ~ "Ouvriers et employés non qualifiés",
                                QUALIF %in% c(3, 4, 6, 7) ~ "Ouvriers et employés qualifiés, techniciens",
                                QUALIF %in% c(8, 9) ~ "Agents de maitrise ou cadres"))


distrib_qualif <- data %>% 
  group_by(qualif_cat) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp))

              
                                        
#=========================================#
### Caractéristiques du droit qui finit ###
#=========================================#


#### Distribution motif de rupture

distrib_passe_pro <- data %>% 
  group_by(passe_pro) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) 



#### Primo-entrant à l'assurance chômage

pjc <- pjc_p %>% 
  select(id_midas, "KROD1_autre" = KROD3, KDFPJ, KCPJC) %>% 
  filter(id_midas %in% id) %>% 
  filter(KCPJC == 1) %>% 
  collect()

droit_passes <- pjc %>% 
  left_join(data %>% select(id_midas, KROD1, date_od), by = c("id_midas")) %>%
  filter(KDFPJ <= date_od & KROD1!=KROD1_autre) %>% 
  distinct(id_midas) %>% 
  distinct(id_midas) %>% 
  mutate(primo_entrant = 0)

data <- data %>% 
  left_join(droit_passes, by = c("id_midas")) %>% 
  mutate(primo_entrant = ifelse(is.na(primo_entrant), 1, primo_entrant))

distrib_primo_entrant <- data %>% 
  group_by(primo_entrant) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp))


#### SJR à l'ouverture

distrib_sjr <- data %>%
  summarise(moyenne_sjr = mean(sjr_ouverture))


#### DPI à l'ouverture
data <- data %>% mutate(dpi_groupe = case_when(
  dpi_ouverture %in% c(0:364) ~ "< 12 mois",
  dpi_ouverture %in% c(365:729) ~ "12 à 24 mois exclu",
  dpi_ouverture %in% c(730) ~ "24 mois",
  dpi_ouverture> 730 ~ "24-36 mois"))


distrib_dpi <- data %>% 
  group_by(dpi_groupe) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp))



#=====================================================================#
### Export final ###
#=====================================================================#

stats_sortants <- list(distrib_age, 
                       distrib_sexe,
                       distrib_nivfor,
                       distrib_qualif,
                       distrib_passe_pro,
                       distrib_sjr,
                       distrib_dpi,
                       distrib_primo_entrant)


write_xlsx(stats_sortants, sprintf(paste0("%s/caracteristiques_stock_",mois, "_",  annee, ".xlsx"), resultats))







