################################################################################
####################### CONSTRUCTION TABLEAU 1 #################################
################################################################################

# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Dans ce programme on compare les caractéristiques individuelles et les 
# caractéristiques des droits (qui se sont achevés) des allocataires
# qui sont sortis d'indemnisation entre juillet et décembre 2022 : 3 groupes 
# (1) sortants qui n'ont pas épuisé leur droit, sorties de moins de 6 mois 
# (2) sortants qui n'ont pas épuisé leur droit, sorties de plus de 6 mois 
# (3) fins de droit non suivies d'un rechargement

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
library(writexl)
library(forcats)
library(tidyr)

# Chemins Dares
path_parquet<- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"
donnees = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"
resultats = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale/"

# Champ temporel
annee <- "2022"
mois <- "07_12"
data <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))
id <- data$id_midas

# Charger les données

pjc_p <- open_dataset(paste0(path_parquet, "/FNA/pjc.parquet"))
odd_p <- open_dataset(paste0(path_parquet, "/FNA/odd.parquet"))


# Identification du champ
data <- data %>% 
  mutate(groupe= case_when(fd == 1 & rechargement == 0 ~ "Fin de droit, non rechargé",
                           fd == 1 & rechargement == 1 ~ "Fin de droit, suivie rechargement",
                           fd == 0 & sortie_longue == 0 ~ "Sortie courte",
                           fd == 0 & sortie_longue == 1 ~ "Sortie longue"))

# Les fins de droit non suivies d'un rechargement ne sont pas dans le champ
data <- data %>% 
  filter(groupe != "Fin de droit, suivie rechargement" & 
           date_fin_pec_ac >= as.Date("2022-07-01") &
           date_fin_pec_ac <= as.Date("2022-12-31"))


#================================#
### Effectifs et parts groupes ###
#================================#

distrib_groupe <- data %>% 
  group_by(groupe) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/nrow(data)) %>%
  select(-n) %>%
  pivot_wider(names_from = "groupe", values_from = "prop") %>%
  mutate(variable = "proportion")


#======================================#
### Nombre de fins de droit par mois ###
#======================================#


n_s2_2022 <- data %>% 
  group_by(groupe, month(date_fin_pec_ac)) %>% 
  summarise(n_tot=n()) 

moy_s2_2022 <- n_s2_2022 %>%
  group_by(groupe) %>%
  summarise(moy = mean(n_tot))  %>%
  mutate(Ensemble = sum(moy)) %>%
  pivot_wider(names_from = "groupe", values_from = "moy") %>%
  mutate(variable = "Effectifs moyens mensuels")


#====================================#
### Caractéristiques individuelles ###
#====================================#

# Récupérer les informations de la table DE
schema_de <- schema("id_midas"= string(), "SEXE" = string(), "NIVFOR"=string(), "DATINS"=date32(), "QUALIF"=string())

de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet), schema=schema_de)

de <- de_p %>% 
  filter(id_midas %in% id) %>%
  collect()

de <- de %>% 
  group_by(id_midas) %>% 
  filter(DATINS==max(DATINS)) %>% 
  filter(!duplicated(id_midas)) # on récupère l'information la plus récente

data <- data %>% 
  left_join(de, by=c("id_midas"))
  
#### Distribution niveau de diplôme

data <- data %>% 
  mutate(diplome_cat = case_when(NIVFOR=="" | is.na(NIVFOR) ~ "Inconnu", 
                                 NIVFOR %in% c("AFS", "CFG", "CP4", "C12", "C3A", "NV5") ~ "Inférieur au BAC-CAP-BEP",
                                 NIVFOR %in% c("NV4") ~"Bac",
                                 NIVFOR %in% c("NV3", "NV2", "NV1") ~"Bac +2 ou plus"),
         diplome_cat = fct_relevel(diplome_cat,
                                   "Inférieur au BAC-CAP-BEP",
                                   "Bac",
                                   "Bac +2 ou plus"))

distrib_nivfor <- data %>% 
  filter(diplome_cat !="Inconnu") %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, diplome_cat) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop) 

distrib_nivfor <- data %>% 
  group_by(diplome_cat) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_nivfor) %>%
  rename(variable = diplome_cat)


#### Distribution de l'âge (à l'ouverture du droit)

age_interval <- c(0,24,52,100)

data <- data %>% 
  mutate(age_tranche = cut(age_od, breaks=age_interval))

distrib_age <- data %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>%
  group_by(groupe, n_gp, age_tranche) %>% 
  summarise(n=n()) %>%
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_age <- data %>% 
  group_by(age_tranche) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_age) %>%
  rename(variable = age_tranche)


#### Distribution sexe

distrib_sexe <- data %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, SEXE) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_sexe <- data %>% 
  group_by(SEXE) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_sexe) %>%
  mutate(SEXE = ifelse(SEXE == "2", "Femmes", "Hommes"))  %>%
  filter(SEXE == "Femmes") %>%
  rename(variable = SEXE)



#### Distribution qualification


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
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, qualif_cat) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_qualif <- data %>% 
  group_by(qualif_cat) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_qualif) %>%
  rename(variable = qualif_cat)

                                                      
#=========================================#
### Caractéristiques du droit qui finit ###
#=========================================#

#### Distribution motif de rupture

distrib_motif_rupture <- data %>%
  mutate(motif_rupture = ifelse(motif_rupture %in% c("Licenciement économique", "Licenciement autre motif"), "Licenciement", motif_rupture),
         motif_rupture = ifelse(motif_rupture == "Rupture d'un commun accord",
                                "Rupture conventionnelle", motif_rupture)) %>%
  mutate(motif_rupture = fct_relevel(motif_rupture,
                                     "Licenciement",
                                     "Fin de contrat",
                                     "Rupture conventionnelle",
                                     "Autre")) %>%
  filter(motif_rupture !="Inconnu") %>% 
  group_by(groupe) %>%
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, motif_rupture) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop) 

distrib_motif_rupture <- data %>%
  mutate(motif_rupture = ifelse(motif_rupture %in% c("Licenciement économique", "Licenciement autre motif"), "Licenciement", motif_rupture),
         motif_rupture = ifelse(motif_rupture == "Rupture d'un commun accord",
                                "Rupture conventionnelle", motif_rupture)) %>%
  mutate(motif_rupture = fct_relevel(motif_rupture,
                                     "Licenciement",
                                     "Fin de contrat",
                                     "Rupture conventionnelle",
                                     "Autre"))  %>% 
  group_by(motif_rupture) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% select(-c(n_gp)) %>% 
  right_join(distrib_motif_rupture) %>%
  rename(variable = motif_rupture)


#### Primo-entrant à l'assurance chômage

pjc <- pjc_p %>% 
  select(id_midas, "KROD1_autre"=KROD3, KDFPJ, KCPJC) %>% 
  filter(id_midas %in% id) %>% 
  filter(KCPJC==1) %>% 
  collect()

droit_passes <- pjc %>% 
  left_join(data %>% 
              select(id_midas, KROD1, date_od, date_fin_pec_ac), by=c("id_midas")) %>%
  filter(KDFPJ <= date_od & KROD1!=KROD1_autre) %>% 
  distinct(id_midas) %>% 
  distinct(id_midas) %>% 
  mutate(primo_entrant=0)

data <- data %>% 
  left_join(droit_passes, by=c("id_midas")) %>% 
  mutate(primo_entrant = ifelse(is.na(primo_entrant), 1, primo_entrant))

distrib_primo_entrant <- data %>% 
  filter(primo_entrant !="Inconnu") %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, primo_entrant) %>% 
  summarise(n=n()) %>% 
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_primo_entrant <- data %>% 
  group_by(primo_entrant) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_primo_entrant) %>%
  filter(primo_entrant == 1) %>%
  mutate(primo_entrant = ifelse(primo_entrant == 1, "primo_entrant", "non_primo_entrant")) %>%
  rename(variable = primo_entrant)

#### SJR à l'ouverture

sjr_ouverture <- odd_p %>% 
  filter(id_midas %in% id) %>%
  select(id_midas, KROD1, "sjr_ouverture"=KQCSJP) %>% 
  collect() 


distrib_sjr <- data %>% 
  left_join(sjr_ouverture, by=c("id_midas", "KROD1")) %>% 
  group_by(groupe) %>%
  summarise(moyenne_sjr = mean(sjr_ouverture)) %>%
  pivot_wider(names_from = "groupe", values_from = moyenne_sjr)

distrib_sjr <- data %>% 
  left_join(sjr_ouverture, by=c("id_midas", "KROD1"))  %>%
  summarise(Ensemble = mean(sjr_ouverture)) %>%
  bind_cols(distrib_sjr) %>%
  mutate(variable = "SJR à l'ouverture moyen")


# DPI à l'ouverture

data <- data %>% mutate(dpi_groupe = case_when(
  dpi_ouverture %in% c(0:364) ~"< 12 mois",
  dpi_ouverture %in% c(365:729) ~"12 à 24 mois exclu",
  dpi_ouverture %in% c(730) ~"24 mois",
  dpi_ouverture> 730 ~ "24-36 mois"),
  dpi_groupe = fct_relevel(dpi_groupe,
                           "< 12 mois",
                           "12 à 24 mois exclu",
                           "24 mois",
                           "24-36 mois"))

distrib_dpi <- data %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, dpi_groupe) %>% 
  summarise(n=n()) %>%
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>%
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_dpi <- data %>% 
  group_by(dpi_groupe) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_dpi) %>%
  rename(variable = dpi_groupe)


#===========================================#
### L'allocataire a-t-il effectué de l'AR ###
#===========================================#

### L'activité réduite est identifiée dans la table PAR (FNA), en filtrant les heures nulles


par_p <- open_dataset(sprintf(paste0("%s/FNA/par.parquet"), path_parquet))

par <- par_p %>%
  filter(id_midas %in% id) %>% 
  filter(KPHARP>0) %>% # filter heures travaillées non-nulles 
  mutate(mois=as.Date(paste0(KDAASP,"-", KDMAP, "-01"))) %>% 
  select(id_midas, mois) %>%
  collect()


### Indicatrice qui vaut 1 si l'individu a fait de l'AR entre la date d'od et la date de sortie
par <- data %>% 
  select(id_midas, date_od, date_fin_pec_ac) %>%
  right_join(par, by=c("id_midas")) 

par <- par %>% 
  filter(date_od <= mois &  date_fin_pec_ac >= mois) %>% 
  mutate(ar=1) %>%  ## idem
  select(id_midas, ar) %>% 
  distinct() 

data <- data %>% 
  left_join(par, by=c("id_midas"))

data <- data %>% 
  mutate(ar=ifelse(is.na(ar), 0, ar))

distrib_ar <- data %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, ar) %>% 
  summarise(n=n()) %>%
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_ar <- data %>% 
  group_by(ar) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_ar) %>%
  filter(ar == 1) %>%
  mutate(ar = ifelse(ar == 1, "activite_reduite","pas_activite_reduite")) %>%
  rename(variable = ar)


#=====================================================================#
### L'allocataire a-t-il effectué une formation au cours du droit ? ###
#=====================================================================#

pjc_aref <- pjc_p %>%
  filter(id_midas %in% id) %>% 
  filter(KCALF %in% c("33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE", "HG")) %>% # AREF (maj code avril 2025)
  filter(KCPJC==1) %>%
  select(id_midas, "KROD1"=KROD3) %>%
  distinct(id_midas, KROD1) %>%
  mutate(formation=1) %>%
  collect() 

data <- pjc_aref %>% 
  select(id_midas, KROD1, formation) %>% 
  distinct() %>%
  right_join(data, by=c("id_midas", "KROD1")) %>%
  mutate(formation=ifelse(is.na(formation), 0, formation))

distrib_formation <- data %>% 
  group_by(groupe) %>% 
  mutate(n_gp = n()) %>% 
  group_by(groupe, n_gp, formation) %>% 
  summarise(n=n()) %>%
  mutate(prop=n*100/n_gp) %>% 
  group_by() %>% 
  select(-c(n, n_gp)) %>% 
  spread(groupe, prop)

distrib_formation <- data %>% 
  group_by(formation) %>% 
  summarise(n_gp=n()) %>%
  mutate(Ensemble=n_gp*100/nrow(data)) %>% 
  group_by() %>% 
  select(-c(n_gp)) %>% 
  right_join(distrib_formation) %>%
  filter(formation == 1) %>%
  mutate(formation = ifelse(formation == 1, "formation","pas_formation")) %>%
  rename(variable = formation)




#==================#
### Export final ###
#==================#

tableau_final <- bind_rows(moy_s2_2022,
                           distrib_groupe,
                           distrib_sexe,
                           distrib_age,
                           distrib_nivfor,
                           distrib_qualif,
                           distrib_primo_entrant,
                           distrib_motif_rupture,
                           distrib_sjr,
                           distrib_dpi,
                           distrib_formation,
                           distrib_ar)

write_xlsx(tableau_final, sprintf(paste0("%s/caracteristiques_sortants_",mois, "_",  annee, ".xlsx"), resultats))







