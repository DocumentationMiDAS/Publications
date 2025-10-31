################################################################################
############################## TABLE FINALE ####################################
################################################################################

# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Ce programme permet de transformer le panel mensuel "calendaire" de suivi des 
# individus qui ont atteint leur fin de droit en un panel "relatif" (à différents 
# horizons relativement à la fin de droit, entre 1 mois avant et 12 mois après)

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
library(ggplot2)
library(readr)
library(stringr)
library(tidyr)

# Chemins

path_parquet<- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"
donnees = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"

# Champ temporel
annee <- "2022"
mois <- "07_12"


# Liste des individus en fin de droit (non rechargé) et date de fin de droit 
# Sur la période temporelle considérée : second semestre 2022
liste_fd <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))
liste_fd <- liste_fd %>% 
  filter(champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") & date_fin_pec_ac <= as.Date("2022-12-31")) %>% 
  select(id_midas, date_fin_pec_ac)
day(liste_fd$date_fin_pec_ac) <- 1

# Panel de suivi mensuel (calendaire)
data <-readRDS(sprintf(paste0("%s/traj_fd.rds"), donnees))


  
# Table d'équivalence des dates
date_deb <- as.Date("2022-01-01")
date_fin <- as.Date("2023-12-31")
debut_mois_liste <- seq.Date(date_deb, date_fin, by="months")
liste_mois_num <- c("0122", "0222", "0322", "0422", "0522", "0622","0722", "0822", "0922", "1022", "1122", "1222",
                    "0123", "0223", "0323", "0423", "0523", "0623", "0723", "0823", "0923", "1023", "1123", "1223")
liste_mois_num_d <- paste0("d", liste_mois_num)
eq_dates <- data.frame(mois_date = debut_mois_liste, mois_num = liste_mois_num_d)

data <- lapply(data, as.data.frame)

data <- lapply(data, function(dataset) {liste_fd %>% left_join(dataset, by=c("id_midas")) %>%
    pivot_longer(starts_with(c("d0", "d1")),
                 names_to = "mois_num",
                 values_to = "valeur")})

data <- lapply(data, function(dataset) {dataset %>% left_join(eq_dates, by=c("mois_num"))})

data <- lapply(data, function(dataset) {dataset %>% distinct()})

data <- lapply(data, function(dataset) {
  # on calcule l'horizon (en mois) par rapport au mois de fin de droit
  dataset$horizon <- (year(dataset$mois_date)-year(dataset$date_fin_pec_ac))*12 + month(dataset$mois_date)-month(dataset$date_fin_pec_ac)  
  dataset$horizon_car <- paste0("h", dataset$horizon) 
  # on transforme la variable d'horizon en character, pour pouvoir la repasser 
  # en colonne (si index "_" = horizon négatif)
  dataset$horizon_car <- gsub("-", "_", dataset$horizon_car) 
  return(dataset)
  })
  
  
data <- lapply(data, function(dataset){dataset %>%
    filter(horizon >= -6 & horizon <= 12) %>% select(-horizon, -mois_num, -mois_date) %>% # on garde un horizon de 6 mois avant et 12 mois après
    pivot_wider(names_from = horizon_car, values_from = valeur) %>% # on repasse la variable d'horizon en colonne 
    select(id_midas, date_fin_pec_ac, h_6, h_5, h_4, h_3, h_2, h_1, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12)
  })


write_rds(data, sprintf(paste0("%s/traj_fd", annee, "_", mois, "_horizon.rds"), donnees))

