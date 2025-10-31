################################################################################
##################### REPERAGE DES SORTANTS ####################################
################################################################################

# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Ce programme permet d'identifier les sortants d'indemnisation (ARE/AREF/ASP), 
# hors intermittents du spectacle, au cours d'un intervalle de temps donné

################################################################################
############################## IMPORTATIONS ####################################
################################################################################

# Vider l'environnement de la session avant de commencer
rm(list=ls())


# Packages nécessaires
library(dplyr)
library(arrow)
library(haven)
library(lubridate)
library(reshape2)
library(duckdb)
library(dbplyr)
library(parquetize)
library(data.table)
library(duckdb)
library(openxlsx)
library(ggplot2)

# Période temporelle de l'étude
date_deb <- as.Date("2022-07-01")
date_fin <- as.Date("2022-12-31")
mois <- "07_12"
annee <- "2022"


# Chemin d'accès aux données
path_parquet <- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"

# Télécharger les tables du FNA de MiDAS (PJC à CDT)
nom_FNA <-list("pjc","odd", "dal", "cdt")
nom_parquet_FNA <- paste0(path_parquet, "/FNA/", nom_FNA, ".parquet") 
nom_import_FNA <-list("pjc_p", "odd_p", "dal_p", "cdt_p") 
for (i in 1 : length(nom_import_FNA)){
  assign(nom_import_FNA[[i]], 
         open_dataset(nom_parquet_FNA[i])
  ); gc()
}

# Télécharger table DE du FHS
schema_de <- schema("id_midas"= string(),"datnais"=date32())
de_p <- open_dataset(paste0(path_parquet, "/FHS/de.parquet"), schema=schema_de)

# Sélectionner les variables d'intérêt ODD CDT 
odd_p <- odd_p %>% 
  select(id_midas, KROD1, KCRD, KQCSJP, KPJDXP, KCCT1, KCAAJ)  

cdt_p <- cdt_p %>% 
  select(id_midas,KCCT, KDFPE, KCMCA)



# Regrouper les codes allocations
pjc_p <- pjc_p %>% 
  mutate(alloc = case_when(KCALF %in% c("01", "21", "22", "27", "40", "43", "47", "54", "64", "67", "82","AC", "BB", "BK", "BP", "CJ", "CN", "CQ", "DM", "EF", "EW", "FL", "GV", "HF")~ "ARE",
                           KCALF %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE", "HG")~ "AREF",
                           KCALF %in% c("AK", "AL", "AM", "AN", "AO","AP","AQ","AR","AS", "BU","BV","BW","BX", "BY","BZ","CA","CB","CC","CD","GC", "GD", "GE", "GG", "GH","GK", "GL",
                                        "FR", "FS", "FT", "FU", "FV", "FW", "FX", "FZ", "GA", "GF", "GG", "GH")~ "ASP",
                           KCALF %in% c("EA", "GI", "EB", "GJ")~ "ATI",
                           KCALF %in% c("25", "30", "44", "51", "56","BD")~ "ASS",
                           TRUE ~ "Autres"))


odd_p <- odd_p %>% 
  mutate(alloc = case_when(KCAAJ %in% c("01", "21", "22", "27", "40", "43", "47", "54", "64", "67", "82","AC", "BB", "BK", "BP", "CJ", "CN", "CQ", "DM", "EF", "EW", "FL", "GV", "HF")~ "ARE",
                           KCAAJ %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE", "HG")~ "AREF",
                           KCAAJ %in% c("AK", "AL", "AM", "AN", "AO","AP","AQ","AR","AS", "BU","BV","BW","BX", "BY","BZ","CA","CB","CC","CD","GC", "GD", "GE", "GG", "GH","GK", "GL",
                                        "FR", "FS", "FT", "FU", "FV", "FW", "FX", "FZ", "GA", "GF", "GG", "GH")~ "ASP",
                           KCAAJ %in% c("EA", "GI", "EB", "GJ")~ "ATI",
                           KCAAJ %in% c("25", "30", "44", "51", "56","BD")~ "ASS",
                           TRUE ~ "Autres"))

################################################################################
##################### CHAMP : FIN DE PEC ARE-ASP ###############################
################################################################################

# Récupérer la variable de régime KCRD dans la table ODD
regime_p <- odd_p %>% 
  select(id_midas, KROD1, KCRD, alloc) 

# Identifier l'ensemble des indemnisables au cours de la période d'intérèt, 
# pour diminuer la taille de l'échantillon
indemnisable <- pjc_p %>% 
  select(id_midas, KROD3, KCPJC, KDDPJ, KDFPJ) %>% 
  filter(KDDPJ <= date_fin & KDFPJ >= date_deb) %>% 
  distinct(id_midas) %>%
  collect() 
id_indemnisable <- indemnisable$id_midas #identifiants individuels des indemnisables
rm(indemnisable)
gc()

# Filtrer sur les ouvertures de droit à l'ARE ou à l'ASP
reperage_pec <- pjc_p %>%
  select(id_midas, KROD3, KDDPJ, KCPJC, KDFPJ, KCFPP) %>% 
  rename(KROD1 = KROD3) %>%
  filter(id_midas %in% id_indemnisable) %>% 
  mutate(id_midas = as.character(id_midas),
         KROD1 = as.character(KROD1)) %>%
  inner_join(regime_p, by = c("id_midas", "KROD1")) %>% 
  filter(alloc %in% c("ARE", "AREF", "ASP")) %>%
  collect() 

# Filtrer pour ne garder que les PJC qui se terminent après 2022
reperage_pec <- reperage_pec %>%
  filter(year(KDFPJ) >= 2022)
gc()

# Calcul de l'écart entre la date de début de PJC et la date de fin de la 
# dernière PJC précédente sur le même droit pour repérer les fins de prises 
# en charge (PEC)
# S'il existe un écart d'au moins un jour entre deux PJC, l'individu n'a pas été 
# pris en charge par l'AC au moins un jour
reperage_pec <- as.data.table(reperage_pec)
setorder(reperage_pec, id_midas, KROD1, KDDPJ)
setorder(reperage_pec, id_midas,  KDDPJ)
reperage_pec[, ecart := as.numeric(KDDPJ - shift(KDFPJ, 1, type = "lag") -1)]
reperage_pec[, date_fin_pec_ac := shift(KDFPJ, 1, type = "lag")]
reperage_pec <- as.data.frame(reperage_pec)


# Connexion à duckdb : pour les opérations par groupe de lignes, duckdb est plus 
# efficient
con <- dbConnect(duckdb::duckdb())
dbWriteTable(con,"reperage_pec", reperage_pec)


# On ne conserve que les fins de PEC suivies d'une reprise (écart d'au moins 
# un jour entre deux PJC)
query <- "
SELECT
  id_midas,
  KROD1,
  KDDPJ,
  date_fin_pec_ac,
  CASE
    WHEN row_number() OVER (PARTITION BY id_midas, KROD1 ORDER BY KDDPJ) = 1 THEN NULL
    ELSE ecart
  END AS ecart,
FROM reperage_pec"

reperage_reprises_droit <- dbGetQuery(con, query)

reperage_reprises_droit_champ <- reperage_reprises_droit %>% 
  filter(ecart > 0 & date_fin_pec_ac >= date_deb & date_fin_pec_ac <= date_fin) %>%
  distinct(id_midas, KROD1, date_fin_pec_ac, ecart)

rm(reperage_reprises_droit)
gc()


# La date de fin de la dernière PJC sur le droit est aussi une fin de PEC
query2 <- "
SELECT
  id_midas,
  KROD1,
  MAX(KDFPJ) AS date_fin_pec_ac
FROM reperage_pec
GROUP BY id_midas, KROD1
HAVING MAX(KDFPJ) BETWEEN ? AND ?"

reperage_non_reprises <- dbGetQuery(con, query2, params = list(date_deb, date_fin))


# Concaténation pour obtenir l'ensemble des fins de PEC : interruptions suivies 
# de reprise et dernière sortie sur le droit
fin_pec <- bind_rows(reperage_non_reprises, reperage_reprises_droit_champ)
gc()


rm(regime_p) 
rm(id_indemnisable)
id_sortants <- fin_pec$id_midas


# Réduire la taille de la table pjc
pjc_p <- pjc_p %>% 
  filter(id_midas %in% id_sortants)


### Caractéristiques du droit associé à la fin de PEC #####

# Ajouter la durée potentielle d'indemnisation à l'ouverture du droit
odd <- odd_p %>% 
  filter(id_midas %in% id_sortants) %>% 
  select(id_midas, KROD1, KPJDXP, KCCT1) %>% 
  collect()

droit <- fin_pec %>% 
  select(id_midas, KROD1) %>%
  inner_join(odd, by = c("id_midas", "KROD1")) %>%
  rename("dpi_ouverture" = "KPJDXP") %>%
  group_by(id_midas, KROD1) %>% 
  filter(dpi_ouverture == max(dpi_ouverture)) %>% 
  filter(!duplicated(id_midas, KROD1)) # pour les quelques doublons de droits qu'il y a, on prend le droit avec la dpi maximale, sinon on sélectionne aléatoireement
rm(odd)

# Ajouter à chaque fin de PEC les caractéristiques du droit associé
sorties_pec_AC <- fin_pec %>% 
  left_join(droit, by = c("id_midas", "KROD1"))


#Caractéristiques du dernier contrat perdu ouvrant le droit à l'AC

cdt <- cdt_p %>%
  filter(id_midas %in% id_sortants) %>% 
  select(id_midas, "KCCT1"=KCCT, KDFPE, KCMCA) %>%
  mutate(KDFPE = as.Date(KDFPE)) %>%
  collect() %>%
  mutate(KCMCA_num = case_when(KCMCA =="XX" ~ 0,
                               KCMCA == ""~0,
                               TRUE ~ as.numeric(KCMCA))) %>%
  mutate(motif_rupture = case_when(KCMCA %in% c(10:16, 23, 24:27, 30,33) ~"Licenciement économique", 
                                   KCMCA %in% c(17:22, 29,42,50, 57, 80, 91,95,116) ~"Licenciement autre motif",
                                   KCMCA %in% c(38,88,92:94,161:175) ~"Rupture conventionnelle",
                                   KCMCA %in% c(40,41,44:45,81,84,154) ~ "Fin de contrat",
                                   TRUE ~"Autre"))
rm(cdt_p)


# Ajouter ces variables pour chaque fin de PEC
sorties_pec_AC <- sorties_pec_AC %>% 
  left_join(cdt, by = c("id_midas", "KCCT1")) 
rm(cdt)


# Calculer la date de l'ouverture du droit
date_od <- pjc_p %>% 
  select(id_midas, KROD3, KDDPJ) %>%
  filter(id_midas %in% id_sortants) %>%
  mutate(KDDPJ = as.Date(KDDPJ)) %>%
  group_by(id_midas, KROD3) %>%
  summarise(date_od = min(KDDPJ)) %>%  
  rename(KROD1 = KROD3) %>%
  collect()

# Ajouter ces variables pour chaque fin de PEC
sorties_pec_AC <- sorties_pec_AC %>% 
  left_join(date_od, by=c("id_midas", "KROD1"))
rm(date_od)
gc()


# Récupérer la date de naissance
de <- de_p %>%   
  filter(id_midas %in% id_sortants) %>% 
  select(id_midas, datnais) %>% 
  collect() %>% 
  filter(!duplicated(id_midas)) 

sorties_pec_AC <- sorties_pec_AC %>% 
  left_join(de, by = c("id_midas")) 
de <- NULL

# Calculer l'âge et corriger la dpi 
sorties_pec_AC <- sorties_pec_AC %>% 
  ungroup() %>% 
  mutate(KDFPE= as.Date(KDFPE),
         age_fc =  ifelse(!is.na(KDFPE) & !is.na(datnais), KDFPE - datnais, NA),
         age_fc = ifelse(is.na(age_fc), date_od - datnais, age_fc),
         age_fc = floor(age_fc/365)) %>%
  mutate(age_od = date_od - datnais,
         age_od = as.numeric(floor(age_od/365)))

sorties_pec_AC <- sorties_pec_AC  %>% 
  mutate(dpi_ouverture = 
           case_when(age_fc < 53 ~ ifelse(dpi_ouverture > 730, 730, dpi_ouverture),
                     age_fc >= 53 & age_fc < 55 ~ ifelse(dpi_ouverture > 913, 913, dpi_ouverture),
                     age_fc >= 55  ~ ifelse(dpi_ouverture > 1095, 1095, dpi_ouverture))) %>%
  filter(dpi_ouverture != 0)


################################################################################
######################### REPERAGE FINS DE DROIT ###############################
################################################################################

# Récupérer l'ensemble des pjc consommées rattachées aux droits sortants
# Rq : un individu peut sur la période d'observation sortir sans avoir tout consommé 
# puis revenir et finir de consommer. Il sera alors compté une fois
# comme une fin de PEC, une fois comme une fin de droit

droit_consomme <- pjc_p %>% 
  mutate(id_midas = as.character(id_midas),
         KROD3 = as.character(KROD3)) %>%
  filter(id_midas %in% id_sortants) %>%
  select(id_midas, "KROD1"=KROD3, KCPJC, KDDPJ, KDFPJ) %>%
  inner_join(fin_pec, by=c("id_midas", "KROD1")) %>%
  filter(KCPJC==1 | KCPJC == 2) %>% 
  collect()
droit_consomme <- as.data.table(droit_consomme)

# Calculer le nombre de jours consommés au cours de la PJC 
# Borner par la période d'intérêt, si la PJC est toujours en cours
droit_consomme <-  droit_consomme[, jours_conso_pjc:=as.numeric(difftime(pmin(KDFPJ, date_fin, date_fin_pec_ac), KDDPJ, units="days") + 1)] 

# En cas de consommation ultérieure à une reprise de droit, la durée est négative : borner à 0
droit_consomme <- droit_consomme %>%
  mutate(jours_conso_pjc = ifelse(jours_conso_pjc < 0, 0, jours_conso_pjc))

# Sommer l'ensemble des PJC relatives à une même fin de PEC
droit_consomme <- droit_consomme %>%
  group_by(id_midas, KROD1, date_fin_pec_ac) %>% 
  summarise(jours_conso_tot = sum(jours_conso_pjc)) %>%
  ungroup

# Calculer le taux de consommation des droits pendant la période d'intérêt
data <- sorties_pec_AC %>% 
  left_join(droit_consomme, by=c("id_midas", "KROD1", "date_fin_pec_ac")) %>%
  mutate(jours_conso_tot = ifelse(is.na(jours_conso_tot), 0, jours_conso_tot))

# Indicatrice fin de droit 
data <- data %>% 
  mutate(fd = ifelse(jours_conso_tot >= dpi_ouverture, 1, 0))


################################################################################
######################### REPERAGE RECHARGEMENTS ###############################
################################################################################

# Repérer une nouvelle ouverture de droit dans les 30 jours
nouveau_droit_temp <- pjc_p %>% 
  select(id_midas, "KROD1"=KROD3, KDDPJ)  %>%
  mutate(id_midas = as.character(id_midas),
         KROD1 = as.character(KROD1))%>%
  filter(id_midas %in% id_sortants) %>%
  left_join(odd_p, by = c("id_midas", "KROD1")) %>%
  filter(alloc %in% c("ARE", "AREF", "ASP")) %>%
  select(id_midas, KROD1, KDDPJ) %>%
  group_by(id_midas, KROD1) %>%
  summarise(date_od_suivante = min(KDDPJ)) %>%
  rename(KROD3_rechargement=KROD1) %>% 
  collect()

nouveau_droit <- data %>% 
  filter(fd == 1) %>%
  rename(date_fin_droit = date_fin_pec_ac) %>%
  left_join(nouveau_droit_temp, by = c('id_midas')) %>%
  filter(date_od_suivante >= date_fin_droit) %>%
  mutate(ecart = date_od_suivante - date_fin_droit) %>%
  filter(ecart <= 30)

rm(nouveau_droit_temp)


rechargement <- nouveau_droit  %>% 
  filter(!duplicated(id_midas, KROD1)) %>%
  mutate(rechargement = 1) %>%
  collect()


# Champ de l'étude : fins de droit non suivies d'un rechargement
# Repérer sortie longue ou non quand le droit n'est pas entièrement consommé
data <- rechargement %>% 
  select(id_midas, KROD1, rechargement) %>% 
  right_join(data, by=c("id_midas", "KROD1"))

data <- data %>% 
  mutate(rechargement = ifelse(is.na(rechargement), 0, rechargement),
         champ = ifelse(fd == 1 & rechargement == 0 , 1, 0))%>%
  mutate(sortie_longue = ifelse(ecart > 182 | (is.na(ecart) & fd == 0), 1, 0)) 



################################################################################
######################### HORS ANNEXES VIII ET X ###############################
################################################################################

# Retirer les annexes 8 et 10
regime <- odd_p %>% 
  select(id_midas, KROD1, KCRD) %>% 
  collect()

data <- data %>% 
  mutate(id_midas = as.character(id_midas),
         KROD1 = as.character(KROD1)) %>%
  inner_join(regime, by = c("id_midas", "KROD1")) %>% 
  filter(KCRD != "28" & KCRD != "29") 

data <- data %>%
  distinct(id_midas, KROD1, date_fin_pec_ac, .keep_all = T)


path_export<- "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"
saveRDS(data, sprintf(paste0("%s/flux_sortants_", mois, "_", annee, ".rds"), path_export))
