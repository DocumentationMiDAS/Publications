################################################################################
################### GRAPHIQUES 2, 3 ; TABLEAUX A, B ############################
################################################################################

# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Dans ce programme on identifie, pour chaque horizon (mensuel) relativement 
# à la fin de droit la part de sortants dans un état donné

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
library(tidyr)
library(writexl)
library(ggpattern)
library(forcats)


# Chemins Dares
path_parquet<- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"
donnees = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"
resultats = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"

# Champ étudié 
annee <- "2022"
mois <- "07_12"

#===========================# 
### Ensemble moins 59 ans ###
#===========================#


# Restriction d'âge : moins de 59 ans lors de la fin de droit
# Pour exclure la plupart des cas de transitions vers la retraite
# Champ temporel : second semestre 2022
age <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))

age <- age %>% 
  mutate(age_fd = floor(as.numeric(date_fin_pec_ac - datnais)/365)) %>% 
  filter(age_fd < 59 & champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") 
         & date_fin_pec_ac <= as.Date("2022-12-31"))

champ_age <- age$id_midas

rm(age)
gc()


# Importer les trajectoires
data <-readRDS(sprintf(paste0("%s/traj_fd", annee, "_", mois, "_horizon.rds"), donnees))
names_data <- names(data)
data <- lapply(data, as.data.frame)

data <- lapply(data, function(dataset) {filter(dataset, id_midas %in% champ_age)})

for(dataset in names_data){
  assign(dataset, data[[dataset]])
}

liste_id <- ass$id_midas

rm(data)
gc()

nrow <- nrow(ass)

# Horizon d'intéret
liste_horizon_car <- c("h_6", "h_5", "h_4", "h_3", "h_2", "h_1", "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12")
eq_date <- data.frame(horizon_car = liste_horizon_car, horizon=c(-6:12))

# Création data
# 1 ligne = 1 individus, 1 mois

data <- data.frame()

for(j in 3:ncol(ass)){
  
  horizon <- names(ass)[j]
  
  ass_ <- ass[,c(1:2,j)] 
  colnames(ass_) <- c("id_midas","dernier_paiement", "ass")
  
  rsa_ <- rsa[,c(1:2,j)]
  colnames(rsa_) <- c("id_midas","dernier_paiement", "rsa")
  
  defm_ <- defm[,c(1:2,j)]
  colnames(defm_) <- c("id_midas", "dernier_paiement","defm")
  
  emploi_durable_ <- emploi_durable[,c(1:2,j)]
  colnames(emploi_durable_) <- c("id_midas", "dernier_paiement","emploi_durable")
  
  emploi_non_durable_ <- emploi_non_durable[,c(1:2,j)]
  colnames(emploi_non_durable_) <- c("id_midas","dernier_paiement", "emploi_non_durable")
  
  data_ <- ass_ %>% 
    left_join(rsa_) %>%
    left_join(defm_) %>%
    left_join(emploi_durable_) %>% 
    left_join(emploi_non_durable_) 
  
  data_$horizon_car <- horizon
  data <- rbind(data_,data)
  
}

rm(data_, activite_e0, are, ass, ass_, rsa, rsa_, defm_, 
   emploi_durable, emploi_durable_, emploi_non_durable, emploi_non_durable_)

gc()


data <- data %>% 
  mutate(across(everything(), ~ replace(., is.na(.), 0)))

data <- data %>% 
  mutate(minima_rsa = ifelse(rsa==1, 1, 0),
         minima_ass = ifelse(rsa==0 & ass==1, 1, 0), 
         minima_aucun = ifelse(rsa == 0 & ass==0, 1, 0),
         
         non_emploi = ifelse(emploi_durable==0 & emploi_non_durable==0, 1, 0),
         
         non_defm = ifelse(defm==0, 1, 0),

         defm_emploi_non_durable = ifelse(defm==1 & emploi_non_durable==1, 1, 0),
         defm_emploi_durable = ifelse(defm==1 & emploi_durable==1, 1, 0),
         defm_non_emploi = ifelse(defm==1 & non_emploi==1, 1, 0),
         
         non_defm_emploi_non_durable = ifelse(non_defm==1 & emploi_non_durable==1, 1, 0),
         non_defm_emploi_durable = ifelse(non_defm==1 & emploi_durable==1, 1, 0),
         non_defm_non_emploi = ifelse(non_defm==1 & non_emploi==1, 1, 0)  )


effectifs <- data %>% 
  group_by(horizon_car) %>% 
  summarize(across(everything(), ~sum(.==1))) %>% 
  print()

total <- data %>% 
  group_by(horizon_car) %>% 
  summarize(n=n())

effectifs <- total %>% 
  left_join(eq_date) %>% 
  left_join(effectifs) %>% 
  select(-id_midas, -dernier_paiement)

effectifs_sortie <- list(effectifs)

write_xlsx(effectifs_sortie, sprintf(paste0("%s/effectifs_traj_agreges.xlsx"), resultats))

effectifs <- effectifs %>% 
  filter(horizon >= -1)

########################### GRAPHIQUE 3 ########################################

effectifs_minima<- effectifs %>% 
  filter(horizon >= -1) %>%
  select(horizon, minima_rsa, minima_ass, minima_aucun) %>% 
  gather(minima, n, -horizon) %>% 
  group_by(horizon, minima) %>%
  mutate(prop = n*100/nrow) %>% 
  arrange(minima, horizon)

effectifs_minima_export <- effectifs_minima %>%
  arrange(horizon, minima) %>%
  select(-n) %>%
  pivot_wider(names_from = "horizon", values_from = "prop" ) %>%
  mutate(minima = case_when(minima == "minima_ass" ~ "ASS",
                            minima == "minima_rsa" ~ "RSA",
                            TRUE ~ "Ni RSA, ni ASS")) %>%
  mutate(minima = as.factor(minima)) %>%
  mutate(minima = fct_relevel(minima, "RSA", "ASS", "Ni RSA, ni ASS")) %>%
  arrange(minima)

write_xlsx(effectifs_minima_export, sprintf(paste0("%s/traj_agree_minima_",mois, "_",  annee, ".xlsx"), resultats))

########################### GRAPHIQUE 2 ########################################
effectifs_defm_emploi <-  effectifs %>%
  filter(horizon >= -1) %>%
  select(horizon, defm_emploi_non_durable,defm_emploi_durable,defm_non_emploi,
         non_defm_emploi_non_durable,non_defm_emploi_durable,non_defm_non_emploi) %>% 
  gather(defm_emploi, n, -horizon) %>% 
  group_by(horizon, defm_emploi) %>%
  mutate(prop = n*100/nrow) %>%  
  arrange(defm_emploi, horizon) %>%
  mutate(emploi_type = case_when(defm_emploi %in% c("non_defm_emploi_durable",
                                                    "defm_emploi_durable") ~ "emploi_durable",
                                 defm_emploi %in% c("non_defm_emploi_non_durable",
                                                    "defm_emploi_non_durable") ~ "emploi_non_durable",
                                 TRUE ~"non_emploi")) %>%
  group_by(horizon, emploi_type) %>%
  mutate(tot = sum(prop)) %>%
  ungroup 

effectifs_defm_emploi <- effectifs_defm_emploi %>%
  select(horizon, defm_emploi, prop) %>%
  bind_rows(effectifs_defm_emploi %>% distinct(horizon, emploi_type, tot) %>%
              transmute(horizon, defm_emploi = emploi_type, prop = tot))
  

effectifs_defm_emploi_export <- effectifs_defm_emploi %>%
  mutate(defm_emploi = fct_relevel(defm_emploi,
                                   "non_defm_emploi_durable",
                                   "defm_emploi_durable",
                                   "emploi_durable",
                                   "non_defm_emploi_non_durable",
                                   "defm_emploi_non_durable",
                                   "emploi_non_durable",
                                   "non_defm_non_emploi",
                                   "defm_non_emploi",
                                   "non_emploi")) %>% 
  pivot_wider(names_from = horizon, values_from = c(prop)) %>%
  arrange(horizon) 

write_xlsx(effectifs_defm_emploi_export, sprintf(paste0("%s/croisement_defm_emploi_",mois, "_",  annee, ".xlsx"), resultats))


#==================# 
### Moins 53 ans ###
#==================#

# Restriction d'âge : moins de 53 ans lors de l'ouverture du droit
# Champ temporel : second semestre 2022
age <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))

age <- age %>% 
  mutate(age_od = floor(as.numeric(date_od - datnais)/365)) %>% 
  filter(age_od < 53 & champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") 
         & date_fin_pec_ac <= as.Date("2022-12-31"))

champ_age <- age$id_midas

rm(age)
gc()


# Importer les trajectoires
data <-readRDS(sprintf(paste0("%s/traj_fd", annee, "_", mois, "_horizon.rds"), donnees))
names_data <- names(data)
data <- lapply(data, as.data.frame)

data <- lapply(data, function(dataset) {filter(dataset, id_midas %in% champ_age)})

for(dataset in names_data){
  assign(dataset, data[[dataset]])
}

liste_id <- ass$id_midas

rm(data)
gc()

nrow <- nrow(ass)

# Horizon d'intéret
liste_horizon_car <- c("h_6", "h_5", "h_4", "h_3", "h_2", "h_1", "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12")
eq_date <- data.frame(horizon_car = liste_horizon_car, horizon=c(-6:12))

# Création data
# 1 ligne = 1 individus, 1 mois

data <- data.frame()

for(j in 3:ncol(ass)){
  
  horizon <- names(ass)[j]
  
  ass_ <- ass[,c(1:2,j)] 
  colnames(ass_) <- c("id_midas","dernier_paiement", "ass")
  
  rsa_ <- rsa[,c(1:2,j)]
  colnames(rsa_) <- c("id_midas","dernier_paiement", "rsa")
  
  emploi_durable_ <- emploi_durable[,c(1:2,j)]
  colnames(emploi_durable_) <- c("id_midas", "dernier_paiement","emploi_durable")
  
  emploi_non_durable_ <- emploi_non_durable[,c(1:2,j)]
  colnames(emploi_non_durable_) <- c("id_midas","dernier_paiement", "emploi_non_durable")
  
  data_ <- ass_ %>% 
    left_join(rsa_) %>%
    left_join(emploi_durable_) %>% 
    left_join(emploi_non_durable_) 
  
  data_$horizon_car <- horizon
  data <- rbind(data_,data)
  
}

rm(data_, activite_e0, are, ass, ass_, rsa, rsa_,
   emploi_durable, emploi_durable_, emploi_non_durable, emploi_non_durable_)

gc()

data <- data %>% 
  mutate(across(everything(), ~ replace(., is.na(.), 0)))

data <- data %>% 
  mutate(minima_rsa = ifelse(rsa==1, 1, 0),
         minima_ass = ifelse(rsa==0 & ass==1, 1, 0), 
         minima_aucun = ifelse(rsa == 0 & ass==0, 1, 0),
         
         non_emploi = ifelse(emploi_durable==0 & emploi_non_durable==0, 1, 0)
         )


effectifs <- data %>% 
  group_by(horizon_car) %>% 
  summarize(across(everything(), ~sum(.==1))) %>% 
  print()

total <- data %>% 
  group_by(horizon_car) %>% 
  summarize(n=n())

effectifs <- total %>% 
  left_join(eq_date) %>% 
  left_join(effectifs) %>% 
  select(-id_midas, -dernier_paiement)

effectifs_sortie <- list(effectifs)

write_xlsx(effectifs_sortie, sprintf(paste0("%s/effectifs_traj_agreges_moins_53.xlsx"), resultats))

effectifs <- effectifs %>% 
  filter(horizon >= -1)

############################# TABLEAU A ########################################
effectifs_minima<- effectifs %>% 
  filter(horizon >= -1) %>%
  select(horizon, minima_rsa, minima_ass, minima_aucun) %>% 
  gather(minima, n, -horizon) %>% 
  group_by(horizon, minima) %>%
  mutate(prop = n*100/nrow) %>% 
  arrange(minima, horizon)

effectifs_minima_export <- effectifs_minima %>%
  arrange(horizon, minima) %>%
  select(-n) %>%
  pivot_wider(names_from = "horizon", values_from = "prop" ) %>%
  mutate(minima = case_when(minima == "minima_ass" ~ "ASS",
                            minima == "minima_rsa" ~ "RSA",
                            TRUE ~ "Ni RSA, ni ASS")) %>%
  mutate(minima = as.factor(minima)) %>%
  mutate(minima = fct_relevel(minima, "RSA", "ASS", "Ni RSA, ni ASS")) %>%
  arrange(minima)


write_xlsx(effectifs_minima_export, sprintf(paste0("%s/traj_agree_minima_",mois, "_",  annee, "_moins_53.xlsx"), resultats))

############################# TABLEAU A ########################################
effectifs_emploi <-  effectifs %>%
  select(horizon, emploi_durable, emploi_non_durable, non_emploi) %>% 
  gather(emploi, n, -horizon) %>% 
  group_by(horizon, emploi) %>%
  mutate(prop = n/nrow) %>%  
  arrange(emploi, horizon)

effectifs_emploi_export <- effectifs_emploi %>%
  pivot_wider(names_from = horizon, values_from = c(n, prop))

write_xlsx(effectifs_emploi_export, sprintf(paste0("%s/emploi_",mois, "_",  annee, "_moins_53.xlsx"), resultats))




#=================# 
### Plus 53 ans ###
#=================#

# Restriction d'âge : plus de 53 ans lors de l'ouverture du droit
# Champ temporel : second semestre 2022
age <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))

age <- age %>% 
  mutate(age_od = floor(as.numeric(date_od - datnais)/365),
         age_fd = floor(as.numeric(date_fin_pec_ac - datnais)/365)) %>% 
  filter(age_od >= 53 & age_fd < 59 & champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") 
         & date_fin_pec_ac <= as.Date("2022-12-31"))

champ_age <- age$id_midas

rm(age)
gc()


# Importer les trajectoires
data <-readRDS(sprintf(paste0("%s/traj_fd", annee, "_", mois, "_horizon.rds"), donnees))
names_data <- names(data)
data <- lapply(data, as.data.frame)

data <- lapply(data, function(dataset) {filter(dataset, id_midas %in% champ_age)})

for(dataset in names_data){
  assign(dataset, data[[dataset]])
}

liste_id <- ass$id_midas

rm(data)
gc()

nrow <- nrow(ass)

# Horizon d'intéret
liste_horizon_car <- c("h_6", "h_5", "h_4", "h_3", "h_2", "h_1", "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12")
eq_date <- data.frame(horizon_car = liste_horizon_car, horizon=c(-6:12))

# Création data
# 1 ligne = 1 individus, 1 mois

data <- data.frame()

for(j in 3:ncol(ass)){
  
  horizon <- names(ass)[j]
  
  ass_ <- ass[,c(1:2,j)] 
  colnames(ass_) <- c("id_midas","dernier_paiement", "ass")
  
  rsa_ <- rsa[,c(1:2,j)]
  colnames(rsa_) <- c("id_midas","dernier_paiement", "rsa")
  
  emploi_durable_ <- emploi_durable[,c(1:2,j)]
  colnames(emploi_durable_) <- c("id_midas", "dernier_paiement","emploi_durable")
  
  emploi_non_durable_ <- emploi_non_durable[,c(1:2,j)]
  colnames(emploi_non_durable_) <- c("id_midas","dernier_paiement", "emploi_non_durable")
  
  data_ <- ass_ %>% 
    left_join(rsa_)%>%
    left_join(emploi_durable_) %>% 
    left_join(emploi_non_durable_) 
  
  data_$horizon_car <- horizon
  data <- rbind(data_,data)
  
}

rm(data_, activite_e0, are, ass, ass_, rsa, rsa_,
   emploi_durable, emploi_durable_, emploi_non_durable, emploi_non_durable_)

gc()

data <- data %>% 
  mutate(across(everything(), ~ replace(., is.na(.), 0)))

data <- data %>% 
  mutate(minima_rsa = ifelse(rsa==1, 1, 0),
         minima_ass = ifelse(rsa==0 & ass==1, 1, 0), 
         minima_aucun = ifelse(rsa == 0 & ass==0, 1, 0),
         
         non_emploi = ifelse(emploi_durable==0 & emploi_non_durable==0, 1, 0))


effectifs <- data %>% 
  group_by(horizon_car) %>% 
  summarize(across(everything(), ~sum(.==1))) %>% 
  print()

total <- data %>% 
  group_by(horizon_car) %>% 
  summarize(n=n())

effectifs <- total %>% 
  left_join(eq_date) %>% 
  left_join(effectifs) %>% 
  select(-id_midas, -dernier_paiement)

effectifs_sortie <- list(effectifs)

write_xlsx(effectifs_sortie, sprintf(paste0("%s/effectifs_traj_agreges_plus_53.xlsx"), resultats))

effectifs <- effectifs %>% 
  filter(horizon >= -1)

############################# TABLEAU A ########################################
effectifs_minima<- effectifs %>% 
  filter(horizon >= -1) %>%
  select(horizon, minima_rsa, minima_ass, minima_aucun) %>% 
  gather(minima, n, -horizon) %>% 
  group_by(horizon, minima) %>%
  mutate(prop = n*100/nrow) %>% 
  arrange(minima, horizon)

effectifs_minima_export <- effectifs_minima %>%
  arrange(horizon, minima) %>%
  select(-n) %>%
  pivot_wider(names_from = "horizon", values_from = "prop" ) %>%
  mutate(minima = case_when(minima == "minima_ass" ~ "ASS",
                            minima == "minima_rsa" ~ "RSA",
                            TRUE ~ "Ni RSA, ni ASS")) %>%
  mutate(minima = as.factor(minima)) %>%
  mutate(minima = fct_relevel(minima, "RSA", "ASS", "Ni RSA, ni ASS")) %>%
  arrange(minima)

write_xlsx(effectifs_minima_export, sprintf(paste0("%s/traj_agree_minima_",mois, "_",  annee, "_plus_53.xlsx"), resultats))

############################# TABLEAU A ########################################
effectifs_emploi <-  effectifs %>%
  select(horizon, emploi_durable, emploi_non_durable, non_emploi) %>% 
  gather(emploi, n, -horizon) %>% 
  group_by(horizon, emploi) %>%
  mutate(prop = n/nrow) %>%  
  arrange(emploi, horizon)

effectifs_emploi_export <- effectifs_emploi %>%
  pivot_wider(names_from = horizon, values_from = c(n, prop))

write_xlsx(effectifs_emploi_export, sprintf(paste0("%s/emploi_",mois, "_",  annee, "_plus_53.xlsx"), resultats))




#==================# 
### Moins 25 ans ###
#==================#

# Restriction d'âge : moins de 25 ans lors de l'ouverture du droit
# Champ temporel : second semestre 2022
age <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))

age <- age %>% 
  mutate(age_od = floor(as.numeric(date_od - datnais)/365)) %>% 
  filter(age_od < 25 & champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") 
         & date_fin_pec_ac <= as.Date("2022-12-31"))

champ_age <- age$id_midas

rm(age)
gc()


# Importer les trajectoires
data <-readRDS(sprintf(paste0("%s/traj_fd", annee, "_", mois, "_horizon.rds"), donnees))
names_data <- names(data)
data <- lapply(data, as.data.frame)

data <- lapply(data, function(dataset) {filter(dataset, id_midas %in% champ_age)})

for(dataset in names_data){
  assign(dataset, data[[dataset]])
}

liste_id <- ass$id_midas

rm(data)
gc()

nrow <- nrow(ass)

# Horizon d'intéret
liste_horizon_car <- c("h_6", "h_5", "h_4", "h_3", "h_2", "h_1", "h0", "h1", "h2", "h3", "h4", "h5", "h6", "h7", "h8", "h9", "h10", "h11", "h12")
eq_date <- data.frame(horizon_car = liste_horizon_car, horizon=c(-6:12))

# Création data
# 1 ligne = 1 individus, 1 mois

data <- data.frame()

for(j in 3:ncol(ass)){
  
  horizon <- names(ass)[j]
  
  ass_ <- ass[,c(1:2,j)] 
  colnames(ass_) <- c("id_midas","dernier_paiement", "ass")
  
  rsa_ <- rsa[,c(1:2,j)]
  colnames(rsa_) <- c("id_midas","dernier_paiement", "rsa")
  
  emploi_durable_ <- emploi_durable[,c(1:2,j)]
  colnames(emploi_durable_) <- c("id_midas", "dernier_paiement","emploi_durable")
  
  emploi_non_durable_ <- emploi_non_durable[,c(1:2,j)]
  colnames(emploi_non_durable_) <- c("id_midas","dernier_paiement", "emploi_non_durable")
  
  data_ <- ass_ %>% 
    left_join(rsa_)  %>%
    left_join(emploi_durable_) %>% 
    left_join(emploi_non_durable_) 
  
  data_$horizon_car <- horizon
  data <- rbind(data_,data)
  
}

rm(data_, activite_e0, are, ass, ass_, rsa, rsa_, 
   emploi_durable, emploi_durable_, emploi_non_durable, emploi_non_durable_)

gc()

data <- data %>% 
  mutate(across(everything(), ~ replace(., is.na(.), 0)))

data <- data %>% 
  mutate(minima_rsa = ifelse(rsa==1, 1, 0),
         minima_ass = ifelse(rsa==0 & ass==1, 1, 0), 
         minima_aucun = ifelse(rsa == 0 & ass==0, 1, 0),
         
         non_emploi = ifelse(emploi_durable==0 & emploi_non_durable==0, 1, 0)
  )


effectifs <- data %>% 
  group_by(horizon_car) %>% 
  summarize(across(everything(), ~sum(.==1))) %>% 
  print()

total <- data %>% 
  group_by(horizon_car) %>% 
  summarize(n=n())

effectifs <- total %>% 
  left_join(eq_date) %>% 
  left_join(effectifs) %>% 
  select(-id_midas, -dernier_paiement)

effectifs_sortie <- list(effectifs)

write_xlsx(effectifs_sortie, sprintf(paste0("%s/effectifs_traj_agreges_moins_25.xlsx"), resultats))

effectifs <- effectifs %>% 
  filter(horizon >= -1)

############################# TABLEAU B ########################################
effectifs_minima<- effectifs %>% 
  filter(horizon >= -1) %>%
  select(horizon, minima_rsa, minima_ass, minima_aucun) %>% 
  gather(minima, n, -horizon) %>% 
  group_by(horizon, minima) %>%
  mutate(prop = n*100/nrow) %>% 
  arrange(minima, horizon)

effectifs_minima_export <- effectifs_minima %>%
  arrange(horizon, minima) %>%
  select(-n) %>%
  pivot_wider(names_from = "horizon", values_from = "prop" ) %>%
  mutate(minima = case_when(minima == "minima_ass" ~ "ASS",
                            minima == "minima_rsa" ~ "RSA",
                            TRUE ~ "Ni RSA, ni ASS")) %>%
  mutate(minima = as.factor(minima)) %>%
  mutate(minima = fct_relevel(minima, "RSA", "ASS", "Ni RSA, ni ASS")) %>%
  arrange(minima)

write_xlsx(effectifs_minima_export, sprintf(paste0("%s/traj_agree_minima_",mois, "_",  annee, "_moins_25.xlsx"), resultats))


############################# TABLEAU B ########################################
effectifs_emploi <-  effectifs %>%
  select(horizon, emploi_durable, emploi_non_durable, non_emploi) %>% 
  gather(emploi, n, -horizon) %>% 
  group_by(horizon, emploi) %>%
  mutate(prop = n/nrow) %>%  
  arrange(emploi, horizon)

effectifs_emploi_export <- effectifs_emploi %>%
  pivot_wider(names_from = horizon, values_from = c(n, prop)) 

write_xlsx(effectifs_emploi_export, sprintf(paste0("%s/emploi_",mois, "_",  annee, "_moins_25.xlsx"), resultats))

