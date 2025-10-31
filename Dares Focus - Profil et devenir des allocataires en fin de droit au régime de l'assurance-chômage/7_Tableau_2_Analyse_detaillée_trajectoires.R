################################################################################
################### CONSTRUCTION TABLEAU 2 TRESOR ECO ##########################
################################################################################


# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Dans ce programme on classe les sortants selon les événements connus au cours
# des 12 mois qui suivent la fin de droit et on étudie leurs caractéristique


###Vider l'environnement de la session avant de commencer
rm(list = ls())

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
library(forcats)

# Chemins Dares
path_parquet<- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5/"
donnees = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"
resultats = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"

# Champ étudié 
annee <- "2022"
mois <- "07_12"

# Restriction d'âge : moins de 59 ans lors de la fin de droit
# Pour exclure les cas de transition vers la retraite
# Champ : second semestre 2022
car <- readRDS(sprintf(paste0("%s/flux_sortants_", mois, "_", annee, ".rds"), donnees))

car <-  car %>% 
  mutate(age_fd = floor(as.numeric(date_fin_pec_ac - datnais)/365)) %>% 
  filter(age_fd < 59 & date_fin_pec_ac >= as.Date("2022-07-01") & 
           date_fin_pec_ac <= as.Date("2022-12-31") & champ == 1)

champ_age <- car$id_midas
gc()

# Charger les données
pjc_p <- open_dataset(paste0(path_parquet, "/FNA/pjc.parquet"))
odd_p <- open_dataset(paste0(path_parquet, "/FNA/odd.parquet"))
par_p <- open_dataset(sprintf(paste0("%s/FNA/par.parquet"), path_parquet))
schema_de <- schema("id_midas" = string(), "SEXE" = string(), "NIVFOR" = string(), "DATINS" = date32(), "QUALIF" = string())
de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet), schema = schema_de)

# Importer les trajectoires
data <-readRDS(sprintf(paste0("%s/traj_fd2022_07_12_horizon.rds"), donnees))
names_data <- names(data)
data <- lapply(data, as.data.frame)

# Filtre age  
data <- lapply(data, function(dataset) {filter(dataset, id_midas %in% champ_age)})

# Filtre horizon (uniquement les événements après la fin de droit)
data <- lapply(data, function(dataset) {select(dataset, c(1:2,9:21))})

for(dataset in names_data){
  assign(dataset, data[[dataset]])
}
rm(data)
gc()

# Créer les indicatrices de cumul

# Nombre de mois indemnisés au RSA
rsa_cumul <- rsa[c(1:3)]
for(j in 4:ncol(rsa)){
  rsa_cumul[,j] <-  rsa_cumul[,j-1] + rsa[,j]
}
rsa_cumul <- rsa_cumul[ ,c(1:2,ncol(rsa))]
colnames(rsa_cumul) <- c("id_midas", "date_fin_pec_ac", "rsa")

# Nombre de mois indemnisés à l'ASS
ass_cumul <- ass[c(1:3)]
for(j in 4:ncol(ass)){
  ass_cumul[,j] <- ass_cumul[,j-1] + ass[,j]
}
ass_cumul <- ass_cumul[ ,c(1:2,ncol(ass))]
colnames(ass_cumul) <- c("id_midas", "date_fin_pec_ac", "ass")

# Nombre de mois indemnisés à l'ARE ou AREF
are_cumul <- are[c(1:3)]
for(j in 4:ncol(are)){
  are_cumul[,j] <-  are_cumul[,j-1] + are[,j]
}
are_cumul <- are_cumul[ ,c(1:2,ncol(are))]
colnames(are_cumul) <- c("id_midas", "date_fin_pec_ac", "are")


# Nombre de mois en emploi durable
emploi_durable_cumul <- emploi_durable[c(1:3)]
for(j in 4:ncol(emploi_durable)){
  emploi_durable_cumul[,j] <-  emploi_durable_cumul[,j-1] + emploi_durable[,j]
}
emploi_durable_cumul <- emploi_durable_cumul[ ,c(1:2,ncol(emploi_durable))]
colnames(emploi_durable_cumul) <- c("id_midas", "date_fin_pec_ac", "emploi_durable")


# Nombre de mois en emploi non durable
emploi_non_durable_cumul <- emploi_non_durable[c(1:3)]
for(j in 4:ncol(emploi_non_durable)){
  emploi_non_durable_cumul[,j] <-  emploi_non_durable_cumul[,j-1] + emploi_non_durable[,j]
}
emploi_non_durable_cumul <- emploi_non_durable_cumul[ ,c(1:2,ncol(emploi_non_durable))]
colnames(emploi_non_durable_cumul) <- c("id_midas", "date_fin_pec_ac", "emploi_non_durable")


data <- ass_cumul %>% 
  left_join(rsa_cumul) %>% 
  left_join(emploi_durable_cumul) %>% 
  left_join(emploi_non_durable_cumul)

data <- data %>% 
  mutate(minima = ifelse(rsa>0 | ass>0, 1, 0),
         emploi_durable_d = ifelse(emploi_durable>0, 1, 0),
         emploi_non_durable_d = ifelse(emploi_non_durable>0, 1, 0))

typ_traj <-data %>% 
  group_by(minima, emploi_non_durable_d, emploi_durable_d) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/nrow(ass)*100) %>% 
  arrange(-prop) %>% 
  group_by() %>%
  mutate(trajectoire = case_when(minima == 0 & emploi_non_durable_d == 0 &
                                   emploi_durable_d == 0 ~ "hors_champ",
                                 minima == 0 & emploi_non_durable_d == 0 &
                                    emploi_durable_d == 1 ~ "insertion_directe",
                                 minima == 1 & emploi_non_durable_d == 0 &
                                   emploi_durable_d == 0 ~ "enfermement_minima",
                                 minima == 0 & emploi_non_durable_d == 1 &
                                   emploi_durable_d == 0 ~ "emploi_non_durable",
                                 minima == 1 & emploi_non_durable_d == 1 &
                                   emploi_durable_d == 0 ~ "emploi_non_durable_minima",
                                 minima == 1 & emploi_non_durable_d == 0 &
                                   emploi_durable_d == 1 ~ "tremplin_minima",
                                 minima == 0 & emploi_non_durable_d == 1 &
                                   emploi_durable_d == 1 ~ "trempin_emploi_non_durable",
                                 minima == 1 & emploi_non_durable_d == 1 &
                                   emploi_durable_d == 1 ~ "trempin_minima_emploi_non_durable"),
         trajectoire = fct_relevel(trajectoire,
                                   "insertion_directe",
                                   "tremplin_minima",
                                   "trempin_emploi_non_durable",
                                   "trempin_minima_emploi_non_durable",
                                   "emploi_non_durable",
                                   "emploi_non_durable_minima",
                                   "enfermement_minima",
                                   "hors_champ")) %>%
  arrange(trajectoire) 

type_traj_export <- typ_traj %>%
  pivot_longer(cols = c("n", "prop", "minima",
                       "emploi_non_durable_d",
                       "emploi_durable_d"),
              names_to = "var", values_to = "val") %>%
  pivot_wider(names_from = trajectoire, values_from = val)

write_xlsx(type_traj_export,
           path = (paste0(resultats, "typologie_trajectoires",mois, "_",  annee,".xlsx")))



# ============================== #
# Caractéristiques des individus #
# ============================== #

# Ajouter la variable de groupe (typologie)

data <- data %>% 
  left_join(typ_traj, by = c("minima", "emploi_durable_d", "emploi_non_durable_d"))

day(car$date_fin_pec_ac) <- 1

data <- data %>% 
  select(id_midas, date_fin_pec_ac, trajectoire) %>% 
  left_join(car, by = c("id_midas", "date_fin_pec_ac"))

id <- data$id_midas



### Récupérer les informations contenues dans la table DE

de <- de_p %>% 
  filter(id_midas %in% id) %>% 
  collect()

de <- de %>% 
  group_by(id_midas) %>% 
  filter(DATINS == max(DATINS))%>% 
  filter(!duplicated(id_midas)) # on récupère l'information la plus récente

data <- data %>% 
  left_join(de, by = c("id_midas"))



### Distribution niveau de diplôme

data <- data %>%
  mutate(diplome_cat = case_when(NIVFOR=="" | is.na(NIVFOR) ~ "Inconnu", 
                                 NIVFOR %in% c("AFS", "CFG", "CP4", "C12", "C3A", "NV5") ~ "Inférieur au bac-CAP-BEP",
                                 NIVFOR %in% c("NV4") ~"Bac",
                                 NIVFOR %in% c("NV3", "NV2", "NV1") ~"Bac +2 ou plus"),
         diplome_cat = fct_relevel(diplome_cat,
                                   "Inférieur au bac-CAP-BEP",
                                   "Bac",
                                   "Bac +2 ou plus"))

distrib_nivfor <- data %>% 
  filter(diplome_cat !="Inconnu") %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, diplome_cat) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_nivfor <- data %>% 
  group_by(diplome_cat) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_nivfor)  %>%
  rename(variable = diplome_cat)



### Distribution de l'âge 

age_interval <- c(0,24,52,100)

data <- data %>% 
  mutate(age_tranche = cut(age_od, breaks = age_interval))

distrib_age <- data %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, age_tranche) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_age <- data %>% 
  group_by(age_tranche) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_age)  %>%
  rename(variable = age_tranche)



### Distribution sexe

distrib_sexe <- data %>% 
  mutate(SEXE = ifelse(SEXE == 1, "hommes", "femmes")) %>%
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, SEXE) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_sexe <- data %>%
  group_by(SEXE) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_sexe)  %>%
  rename(variable = SEXE)



### Distribution qualification

data <- data %>% 
  mutate(qualif_cat = case_when(QUALIF==0 | is.na(QUALIF) ~ "Inconnu", 
                                QUALIF %in% c(1, 2, 5) ~ "Ouvriers et employés non qualifiés",
                                QUALIF %in% c(3, 4, 6, 7) ~"Ouvriers et employés qualifiés, techniciens",
                                QUALIF %in% c(8, 9) ~"Agents de maitrise ou cadres"),
         qualif_cat = fct_relevel(qualif_cat,
                                  "Ouvriers et employés non qualifiés",
                                  "Ouvriers et employés qualifiés, techniciens",
                                  "Agents de maitrise ou cadres"))

distrib_qualif <- data %>% 
  filter(qualif_cat !="Inconnu") %>%
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, qualif_cat) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_qualif <- data %>% 
  group_by(qualif_cat) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_qualif)  %>%
  rename(variable = qualif_cat)



#=========================================#
### Caracétristiques du droit qui finit ###
#=========================================#

### Distribution motif de rupture

distrib_motif_rupture <- data %>% 
  mutate(motif_rupture = case_when(motif_rupture %in% c("Licenciement économique",
  "Licenciement autre motif") ~"Licenciement",
  TRUE ~ motif_rupture),
  motif_rupture = fct_relevel(motif_rupture,
                              "Licenciement",
                              "Fin de contrat",
                              "Rupture d'un commun accord",
                              "Autre")) %>%
  arrange(motif_rupture) %>%
  filter(motif_rupture != "Inconnu") %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, motif_rupture) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop) 

distrib_motif_rupture <- data %>%
  group_by(motif_rupture) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_motif_rupture)  %>%
  rename(variable = motif_rupture)


### Primo-entrant à l'assurance chômage

pjc <- pjc_p %>% 
  select(id_midas, "KROD1_autre"=KROD3, KDFPJ, KCPJC) %>% 
  filter(id_midas %in% id) %>%
  filter(KCPJC == 1) %>% 
  collect()

droit_passes <- pjc %>%
  left_join(data %>% 
              select(id_midas, KROD1, date_od, date_fin_pec_ac), by = c("id_midas")) %>% ### CORR DATE
  filter(KDFPJ <= date_od & KROD1 != KROD1_autre) %>% 
  distinct(id_midas) %>% 
  distinct(id_midas) %>% 
  mutate(primo_entrant = 0)

data <- data %>% 
  left_join(droit_passes, by = c("id_midas")) %>%
  mutate(primo_entrant = ifelse(is.na(primo_entrant), 1, primo_entrant))

distrib_primo_entrant <- data %>% 
  filter(primo_entrant != "Inconnu") %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, primo_entrant) %>% 
  summarise(n = n()) %>% 
  mutate(prop = n/n_gp) %>% 
  group_by() %>%
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_primo_entrant <- data %>%
  group_by(primo_entrant) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_primo_entrant)  %>%
  filter(primo_entrant == 1) %>%
  mutate(variable = "primo_entrant") %>%
  select(-primo_entrant)


### SJR à l'ouverture : moyenne par groupe

sjr_ouverture <- odd_p %>% 
  filter(id_midas %in% id) %>%
  select(id_midas, KROD1, "sjr_ouverture"=KQCSJP) %>% 
  collect() 

data <- data %>%
  left_join(sjr_ouverture, by=c("id_midas", "KROD1"))

distrib_sjr_moy <- data %>% 
  group_by(trajectoire) %>%
  summarise(sjr_moy = mean(sjr_ouverture, na.rm = T)) %>%
  pivot_wider(names_from = "trajectoire", values_from = "sjr_moy")  %>%
  mutate(variable = "sjr_moy")


### DPI à l'ouverture

data <- data %>% 
  mutate(dpi_trajectoire = case_when(
  dpi_ouverture%in% c(0:364) ~"moins de 12 mois",
  dpi_ouverture %in% c(365:729) ~"12 à 24 mois exclus",
  dpi_ouverture %in% c(730) ~"24 mois",
  dpi_ouverture> 730 ~ "24 à 36 mois"),
  dpi_trajectoire = fct_relevel(dpi_trajectoire,
                                "moins de 12 mois",
                                "12 à 24 mois exclus",
                                "24 mois",
                                "24 à 36 mois"))

distrib_dpi <- data %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, dpi_trajectoire) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>%
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_dpi <- data %>% 
  group_by(dpi_trajectoire) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_dpi)  %>%
  rename(variable = dpi_trajectoire)



#===========================================#
### L'allocataire a-t-il effectué de l'AR ###
#===========================================#

### L'activité réduite est identifiée dans la table PAR (FNA), en filtrant les heures nulles

par <- par_p %>%
  filter(id_midas %in% id) %>% 
  filter(KPHARP>0) %>% # filter heures travaillées non-nulles 
  mutate(mois = as.Date(paste0(KDAASP,"-", KDMAP, "-01"))) %>% 
  select(id_midas, mois) %>%
  collect()



# Indicatrice qui vaut 1 si l'individu a fait de l'AR entre la date d'od et la date de sortie

par <- data %>%
  select(id_midas, date_od, date_fin_pec_ac) %>% 
  right_join(par, by = c("id_midas"))

par <- par %>%
  filter(date_od <= mois & date_fin_pec_ac >= mois) %>%
  mutate(ar = 1) %>% 
  select(id_midas, ar) %>%
  distinct() 

data <- data %>%
  left_join(par, by = c("id_midas"))

data <- data %>% 
  mutate(ar = ifelse(is.na(ar), 0, ar))

distrib_ar <- data %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, ar) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_ar <- data %>% 
  group_by(ar) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_ar)  %>%
  filter(ar == 1) %>%
  mutate(variable = "ar") %>%
  select(-ar)



#=====================================================================#
### L'allocataire a-t-il effectué une formation au cours du droit ? ###
#=====================================================================#

### Indemnisation à l'AREF

pjc_aref <- pjc_p %>%
  filter(id_midas %in% id) %>% 
  filter(KCALF %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE")) %>% # AREF (maj code avril 2025)
  filter(KCPJC == 1) %>%
  select(id_midas, "KROD1"=KROD3) %>%
  distinct(id_midas, KROD1) %>%
  mutate(formation = 1) %>%
  collect() 

data <- pjc_aref %>%
  select(id_midas, KROD1, formation) %>% 
  distinct() %>%
  right_join(data, by = c("id_midas", "KROD1")) %>%
  mutate(formation = ifelse(is.na(formation), 0, formation))

# Part des individus qui ont touché de l'aref pendant leur droit 

distrib_formation <- data %>% 
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, formation) %>%
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_formation <- data %>% 
  group_by(formation) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>%
  select(-c(n_gp)) %>% 
  right_join(distrib_formation) %>%
  filter(formation == 1) %>%
  mutate(variable = "formation") %>%
  select(-formation)




# ============================== #
# Outcomes (trajectoires futures) #
# ============================== #


### Qui est en emploi durable à m+12

emploi_durable_m12 <- emploi_durable %>% 
  select(id_midas, date_fin_pec_ac, emploi_durable_m12 = h12)

data <- data %>% 
  left_join(emploi_durable_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_emploi_durable_m12 <- data %>%
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, emploi_durable_m12) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_emploi_durable_m12 <- data %>% 
  group_by(emploi_durable_m12) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>%
  select(-c(n_gp)) %>%
  right_join(distrib_emploi_durable_m12) %>%
  filter(emploi_durable_m12 == 1)  %>%
  mutate(variable = ifelse(emploi_durable_m12 == 1, "emploi_durable_m12", "0")) %>%
  select(-emploi_durable_m12)



### Qui est en emploi non durable à m+12

emploi_non_durable_m12 <- emploi_non_durable %>% 
  select(id_midas, date_fin_pec_ac, emploi_non_durable_m12 = h12)

data <- data %>% 
  left_join(emploi_non_durable_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_emploi_non_durable_m12 <- data %>%
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, emploi_non_durable_m12) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_emploi_non_durable_m12 <- data %>%
  group_by(emploi_non_durable_m12) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>%
  right_join(distrib_emploi_non_durable_m12) %>%
  filter(emploi_non_durable_m12 == 1)  %>%
  mutate(variable = ifelse(emploi_non_durable_m12 == 1, "emploi_non_durable_m12", "0")) %>%
  select(-emploi_non_durable_m12)



### Qui touche le RSA à m+12

rsa_m12 <- rsa %>% 
  select(id_midas, date_fin_pec_ac, rsa_m12 = h12)

data <- data %>% 
  left_join(rsa_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_rsa_m12 <- data %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, rsa_m12) %>%
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_rsa_m12 <- data %>%
  group_by(rsa_m12) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>%
  select(-c(n_gp)) %>% 
  right_join(distrib_rsa_m12) %>%
  filter(rsa_m12 == 1) %>%
  mutate(variable = ifelse(rsa_m12 == 1, "rsa_m12", "0")) %>%
  select(-rsa_m12)



### Qui touche l'ass à m+12

ass_m12 <- ass %>%
  select(id_midas, date_fin_pec_ac, ass_m12 = h12)

data <- data %>% 
  left_join(ass_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_ass_m12 <- data %>% 
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, ass_m12) %>%
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(trajectoire, prop)

distrib_ass_m12 <- data %>% 
  group_by(ass_m12) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp))%>% 
  right_join(distrib_ass_m12) %>%
  filter(ass_m12 == 1) %>%
  mutate(variable = ifelse(ass_m12 == 1, "ass_m12", "0")) %>%
  select(-ass_m12)




### Qui touche l'ARE ou AREF à m+12

are_m12 <- are %>% 
  select(id_midas, date_fin_pec_ac, are_m12 = h12)

data <- data %>%
  left_join(are_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_are_m12 <- data %>%
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, are_m12) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_are_m12 <- data %>% 
  group_by(are_m12) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>%
  select(-c(n_gp)) %>%
  right_join(distrib_are_m12)  %>%
  filter(are_m12 == 1) %>%
  mutate(variable = ifelse(are_m12 == 1, "are_m12", "0")) %>%
  select(-are_m12)



### Qui touche la prime d'activité à m+12

ppa_m12 <- ppa %>% 
  select(id_midas, date_fin_pec_ac, ppa_m12 = h12)

data <- data %>% 
  left_join(ppa_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_ppa_m12 <- data %>% 
  group_by(trajectoire) %>% 
  mutate(n_gp = n()) %>% 
  group_by(trajectoire, n_gp, ppa_m12) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>%
  group_by() %>%
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_ppa_m12 <- data %>% 
  group_by(ppa_m12) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) %>%
  right_join(distrib_ppa_m12) %>%
  filter(ppa_m12 == 1) %>%
  mutate(variable = ifelse(ppa_m12 == 1, "ppa_m12", "0")) %>%
  select(-ppa_m12)



### Qui  est DEFM A à m+12

defma_m12 <- defm_a %>% 
  select(id_midas, date_fin_pec_ac, defma_m12 = h12)

data <- data %>% 
  left_join(defma_m12, by = c("id_midas", "date_fin_pec_ac"))


distrib_defma_m12 <- data %>% 
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, defma_m12) %>%
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_defma_m12 <- data %>% 
  group_by(defma_m12) %>%
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>% 
  group_by() %>%
  select(-c(n_gp)) %>%
  right_join(distrib_defma_m12)  %>%
  filter(defma_m12 == 1) %>%
  mutate(variable = ifelse(defma_m12 == 1, "defma_m12", "0")) %>%
  select(-defma_m12)



### Qui  est DEFM BC à m+12

defmbc_m12 <- defm_bc %>% 
  select(id_midas, date_fin_pec_ac, defmbc_m12 = h12)

data <- data %>% 
  left_join(defmbc_m12, by = c("id_midas", "date_fin_pec_ac"))

distrib_defmbc_m12 <- data %>% 
  group_by(trajectoire) %>%
  mutate(n_gp = n()) %>%
  group_by(trajectoire, n_gp, defmbc_m12) %>% 
  summarise(n = n()) %>%
  mutate(prop = n/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>%
  spread(trajectoire, prop)

distrib_defmbc_m12 <- data %>% 
  group_by(defmbc_m12) %>% 
  summarise(n_gp = n()) %>%
  mutate(Ensemble = n_gp/nrow(data)) %>%
  group_by() %>% 
  select(-c(n_gp)) %>%
  right_join(distrib_defmbc_m12)  %>%
  filter(defmbc_m12 == 1) %>%
  mutate(variable = ifelse(defmbc_m12 == 1, "defmbc_m12", "0")) %>%
  select(-defmbc_m12)



#=====================================================================#
### Export final ###
#=====================================================================#

tableau_final <- bind_rows(distrib_sexe,
                           distrib_age,
                           distrib_nivfor,
                           distrib_qualif,
                           distrib_primo_entrant,
                           distrib_motif_rupture,
                           distrib_sjr_moy,
                           distrib_dpi,
                           distrib_formation,
                           distrib_ar,
                           distrib_defma_m12,
                           distrib_defmbc_m12,
                           distrib_emploi_durable_m12,
                           distrib_emploi_non_durable_m12,
                           distrib_are_m12,
                           distrib_ppa_m12,
                           distrib_rsa_m12,
                           distrib_ass_m12)

write_xlsx(tableau_final, sprintf(paste0("%s/caracteristiques_trajectoire_sortie_",mois, "_",  annee, ".xlsx"), resultats))


