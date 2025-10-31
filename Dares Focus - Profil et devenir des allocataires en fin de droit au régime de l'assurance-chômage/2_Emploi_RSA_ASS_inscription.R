################################################################################
######################### DEVENIR DES SORTANTS #################################
################################################################################

# J. DUCOULOMBIER, I. GLASER et L. FAUVRE
# 
# Dans ce programme on identifie, pour chaque fin de mois, la situation des individus du champ
# 1 table = 1 info sur une prestation, chaque colonne = situation 1 mois donné

################################################################################
############################## IMPORTATIONS ####################################
################################################################################

### Vider l'environnement de la session avant de commencer
rm(list=ls())
gc()

### Packages nécessaires
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
library(openxlsx)
library(tidyr)

### Chemins Dares
path_parquet<- "C:/Users/Public/Documents/MiDAS_parquet/Vague 5"
donnees = "C:/Users/MIDARES_L_FAUVRE0/Documents/Fins de droit copublication Trésor/Codes_version_finale"

#### Champ étudié
annee <- "2022"
mois <- "07_12"

data <- readRDS(sprintf(paste0("%s/flux_sortants_07_12_2022.rds"), donnees))
data <- data %>% 
  filter(champ == 1 & date_fin_pec_ac >= as.Date("2022-07-01") & date_fin_pec_ac <= as.Date("2022-12-31")) %>% 
  distinct(id_midas)

champ <- data$id_midas

#### Horizon d'intéret
date_deb <- as.Date("2022-01-01")
date_fin <- as.Date("2023-12-31")

debut_mois_liste <- seq.Date(date_deb, date_fin, by="months")

liste_mois_num <- c("0122", "0222", "0322", "0422", "0522", "0622", "0722", "0822", "0922", "1022", "1122", "1222",
                    "0123", "0223", "0323", "0423", "0523", "0623", "0723", "0823", "0923", "1023", "1123", "1223")
liste_mois_num_d <- paste0("d", liste_mois_num)

#=================#
#### Table RSA ###
#=================#

rsa <- data

for(mois in liste_mois_num){

  schema_cnaf <- schema("id_midas"= string(), "MTRSAVER"=double(), "RSAVERS"= string())
 
  rsa_p <- open_dataset(sprintf(paste0("%s/Allstat/minsoc/cnaf_indiv_", mois, ".parquet"), path_parquet), schema=schema_cnaf)
  
  rsa_ <- rsa_p %>%
    filter(RSAVERS %in% c("RSA droit commun", "RSA expérimental", "RSA jeune", "RSA droit local", "C", "E", "J", "L")) %>% 
    filter(id_midas %in% champ) %>% 
    filter(MTRSAVER > 0) %>% 
    mutate(rsa = 1) %>% 
    select(id_midas, rsa) %>%
    collect() %>% 
    filter(!duplicated(id_midas))
  colnames(rsa_) <-  c("id_midas", paste0("d", mois))
  
  rsa <- merge(rsa, rsa_ ,by=c("id_midas"), all.x=T)
  print(mois)
}

rsa[is.na(rsa)] <-0
colSums(rsa[,-1])
rm(rsa_)



#=================#
#### Table PPA ###
#=================#

ppa <- data

for(mois in liste_mois_num){
  
  schema_cnaf <- schema("id_midas"= string(), "MTPPAVER"=double(), "PPAVERS" = string())
  #ppa_p <- open_dataset(sprintf(paste0("%s/cnaf_indiv_", mois, ".parquet"), path_parquet), schema=schema_cnaf)
  ppa_p <- open_dataset(sprintf(paste0("%s/Allstat/minsoc/cnaf_indiv_", mois, ".parquet"), path_parquet), schema=schema_cnaf)
  
  ppa_ <- ppa_p %>% 
    filter(PPAVERS %in% c("Prime d'Activité non majorée 18-25 ans", "Prime d'Activité non majorée 25 ans et +", "Prime d'Activité majorée", "1", "2", "3", "1.0", "2.0", "3.0") | 
             PPAVERS %in% c(1, 2, 3)) %>%
    filter(id_midas %in% champ) %>% 
    filter(MTPPAVER > 0) %>% 
    mutate(ppa = 1) %>% 
    select(id_midas, ppa) %>%
    collect() %>% 
    filter(!duplicated(id_midas))
  colnames(ppa_) <-  c("id_midas", paste0("d", mois))
  
  ppa <- merge(ppa, ppa_ ,by=c("id_midas"), all.x=T)
  print(mois)
}

ppa[is.na(ppa)] <-0
colSums(ppa[,-1])
rm(ppa_)



#=================#
#### Table ASS ###
#=================#

pjc_p <- open_dataset(paste0(path_parquet, "/FNA/pjc.parquet"))

pjc_ass <- pjc_p %>%
  select(id_midas, KDDPJ, KDFPJ, KCPJC, KCALF) %>%
  filter(id_midas %in% champ) %>% 
  filter(KCALF %in% c("25", "30", "44", "51", "56","BD")) %>% 
  collect() 

# Perception (ou non) de l'ass (payée) PENDANT LE MOIS
ass <- data

for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  
  ass_ <- pjc_ass %>% 
    select(id_midas, KDDPJ, KDFPJ, KCPJC) %>%
    filter(KDDPJ <= fin_mois & KDFPJ >= debut_mois) %>%
    filter(KCPJC==1 | KCPJC == 4) %>%
    select(-c(KDDPJ, KDFPJ, KCPJC)) %>% 
    mutate(ass = 1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(ass_) <-  c("id_midas", debut_mois)
  
  ass <- merge(ass,ass_, all.x=T)
  print(as.Date(fin_mois))

}

colnames(ass) <- c("id_midas", liste_mois_num_d)

ass[is.na(ass)] <-0
colSums(ass[,-1])
rm(ass_)



#=====================#
#### Table ARE-AREF ###
#=====================#

# Importer les pjc qui correspondent à l'are et asp
pjc_are <- pjc_p %>%
  filter(id_midas %in% champ) %>% 
  filter(KCPJC==1 | KCPJC == 4) %>%
  #maj code allocations ass (avril 2025)
  filter(KCALF %in% c("01", "21", "22", "27", "40", "43", "47", "54", "64", "67", "82","AC", "BB", "BK", "BP", "CJ", "CN", "CQ", "DM", "EF", "EW", "FL", "GV", "HF",  # ARE
                      "33", "34", "35", "48", "55", "65", "83", "AD", "BC", "BN", "BQ", "CK", "CO", "CR", "DQ", "EL", "EX", "FO", "GZ", "HE", "HG", # AREF
                      "AK", "AL", "AM", "AN", "AO","AP","AQ","AR","AS", "BU","BV","BW","BX", "BY","BZ","CA","CB","CC","CD","GC", "GD", "GE", "GG", "GH","GK", "GL",
                      "FR", "FS", "FT", "FU", "FV", "FW", "FX", "FZ", "GA", "GF", "GG", "GH")) %>% #ASP
  collect() 

# Perception (ou non) de l'are (payée) PENDANT LE MOIS
are <- data

for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  
  are_ <- pjc_are %>% 
    select(id_midas, KDDPJ, KDFPJ, KCPJC) %>%
    filter(KDDPJ <= fin_mois & KDFPJ >= debut_mois) %>%
    filter(KCPJC == 1 | KCPJC == 4) %>%
    select(-c(KDDPJ, KDFPJ, KCPJC)) %>% 
    mutate(are = 1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(are_) <-  c("id_midas", debut_mois)
  
  are <- merge(are,are_, all.x=T)
  print(as.Date(fin_mois))
  
}

colnames(are) <- c("id_midas", liste_mois_num_d)

are[is.na(are)] <-0
colSums(are[,-1])
rm(are_)




#=================#
#### Table e0 #####
#=================#


# Activité réduite

e0_p <- open_dataset(sprintf(paste0("%s/FHS/e0.parquet"), path_parquet))


e0 <- e0_p %>% 
  select(-IDX, -REGION, -NDEM) %>%
  filter(id_midas %in% champ) %>% 
  mutate(annee = substr(MOIS, 1,4),
         mois = substr(MOIS, 5,6)) %>%
  filter(annee == 2022 | annee == 2023) %>% 
  select(-MOIS) %>%
  collect() 

e0 <- e0 %>% 
  mutate(date = as.Date(paste(annee, mois, "01", sep="-"))) %>% 
  select(-annee, -mois)

# Activité réduite au sens de e0 ou non

activite_e0 <- data

for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)

  activite_e0_ <- e0 %>% 
    select(id_midas,date) %>%
    filter(debut_mois == date) %>% 
    select(-date) %>% 
    mutate(activite_e0 = 1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(activite_e0_) <-  c("id_midas", debut_mois)
  
  activite_e0 <- merge(activite_e0,activite_e0_, all.x=T)
  print(as.Date(debut_mois))
  
}

colnames(activite_e0) <- c("id_midas", liste_mois_num_d)
activite_e0[is.na(activite_e0)] <-0
colSums(activite_e0[,-1])
rm(activite_e0_, e0)
gc()


#=====================#
#### Table DEFM ABC ###
#=====================#

# Demandes d'emploi (ABC) des individus d'intérèt

schema_de <- schema("id_midas"= string(), "DATINS"=date32(), "DATANN"=date32(), "CATREGR"=string())

de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet),  schema=schema_de)

de <- de_p %>% 
  filter(id_midas %in% champ) %>% 
  filter(CATREGR%in% c("1", "2","3")) %>%
  collect() 

defm_abc <- data
for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  
  defm_abc_ <-de %>% 
    select(id_midas,DATINS, DATANN) %>%
    filter(DATINS <= fin_mois & (DATANN >= fin_mois | is.na(DATANN) )) %>% 
    select(-DATINS, -DATANN) %>% 
    mutate(defm_abc=1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(defm_abc_) <-  c("id_midas", debut_mois)
  
  defm_abc <- merge(defm_abc,defm_abc_, all.x=T)
  print(as.Date(fin_mois))
  
}

colnames(defm_abc) <- c("id_midas", liste_mois_num_d)
defm_abc[is.na(defm_abc)] <-0
colSums(defm_abc[,-1])
rm(de, defm_abc_)

# On croise avec la table d'activité réduite pour distinguer les DEFM A et BC

defm_a <- data %>% 
  arrange(id_midas)
defm_bc <- data %>% 
  arrange(id_midas)
activite_e0 <- activite_e0 %>% 
  arrange(id_midas)

for(mois in liste_mois_num_d){
  defm_a[[mois]] <- (defm_abc[[mois]]==1) & (activite_e0[[mois]]==0)
  defm_bc[[mois]] <- (defm_abc[[mois]]==1) & (activite_e0[[mois]]==1)
  defm_a[[mois]] <- as.integer(defm_a[[mois]])
  defm_bc[[mois]] <- as.integer(defm_bc[[mois]])
}

colnames(defm_a) <- c("id_midas", liste_mois_num_d)
colnames(defm_bc) <- c("id_midas", liste_mois_num_d)

colSums(defm_a[,-1])
colSums(defm_bc[,-1])


#=================#
#### Table par ####
#=================#


# Activité réduite

par_p <- open_dataset(sprintf(paste0("%s/FNA/par.parquet"), path_parquet))

par <- par_p %>% 
  filter(KQBARP > 0 & KPHARP > 0) %>%
  filter(id_midas %in% champ) %>% 
  rename(annee = KDAASP,
         mois = KDMAP) %>%
  filter(annee == 2022 | annee == 2023) %>%
  select(-KQBARP) %>%
  collect() 

par <- par %>% 
  mutate(date = as.Date(paste(annee, mois, "01", sep="-"))) %>% 
  select(-annee, -mois)

# Activité réduite au sens de par ou non

activite_par <- data

for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  
  activite_par_ <- par %>% 
    select(id_midas,date) %>%
    filter(debut_mois == date) %>% 
    select(-date) %>%
    mutate(activite_par=1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(activite_par_) <-  c("id_midas", debut_mois)
  
  activite_par <- merge(activite_par, activite_par_, all.x=T)
  print(as.Date(debut_mois))
  
}

colnames(activite_par) <- c("id_midas", liste_mois_num_d)
activite_par[is.na(activite_par)] <-0
colSums(activite_par[,-1])
rm(activite_par_, par)
gc()



#===================#
### Table DEFM DE ###
#===================#

# Demandes d'emploi (DE) des individus d'intérèt

schema_de <- schema("id_midas"= string(), "DATINS"=date32(), "DATANN"=date32(), "CATREGR"=string())

de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet),  schema=schema_de)

de <- de_p %>% 
  filter(id_midas %in% champ) %>% 
  filter(CATREGR %in% c("4", "5")) %>%
  collect() 

defm_de <- data
for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  
  defm_de_ <- de %>% 
    select(id_midas, DATINS, DATANN) %>%
    filter(DATINS <= fin_mois & (DATANN >= fin_mois | is.na(DATANN) )) %>% 
    select(-DATINS, -DATANN) %>% 
    mutate(defm_de = 1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(defm_de_) <-  c("id_midas", debut_mois)
  
  defm_de <- merge(defm_de,defm_de_, all.x=T)
  print(as.Date(fin_mois))
  
}

colnames(defm_de) <- c("id_midas", liste_mois_num_d)
defm_de[is.na(defm_de)] <-0
colSums(defm_de[,-1])
rm(de, defm_de_)

# On croise avec la table d'activité réduite par

defm_de_non_ar <- data %>% 
  arrange(id_midas)
defm_de_ar <- data %>% 
  arrange(id_midas)
activite_par <- activite_par %>% 
  arrange(id_midas)

for(mois in liste_mois_num_d){
  defm_de_non_ar[[mois]] <- (defm_de[[mois]]==1) & (activite_par[[mois]]==0)
  defm_de_ar[[mois]] <- (defm_de[[mois]]==1) & (activite_par[[mois]]==1)
  defm_de_non_ar[[mois]] <- as.integer(defm_de_non_ar[[mois]])
  defm_de_ar[[mois]] <- as.integer(defm_de_ar[[mois]])
}

colnames(defm_de_non_ar) <- c("id_midas", liste_mois_num_d)
colnames(defm_de_ar) <- c("id_midas", liste_mois_num_d)

colSums(defm_de_non_ar[,-1])
colSums(defm_de_ar[,-1])

#==================#
#### Table DEFM ####
#==================#


# Demandes d'emploi (DE) des individus d'intérèt

schema_de <- schema("id_midas"= string(), "DATINS"=date32(), "DATANN"=date32(), "CATREGR"=string())

de_p <- open_dataset(sprintf(paste0("%s/FHS/de.parquet"), path_parquet),  schema=schema_de)

de <- de_p %>% 
  filter(id_midas %in% champ)%>%
  collect() 

defm <- data
for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  
  defm_ <- de %>% 
    select(id_midas,DATINS, DATANN) %>%
    filter(DATINS <= fin_mois & (DATANN >= fin_mois | is.na(DATANN) )) %>% 
    select(-DATINS, -DATANN) %>% 
    mutate(defm = 1) %>% 
    filter(!duplicated(id_midas))
  
  colnames(defm_) <-  c("id_midas", debut_mois)
  
  defm <- merge(defm,defm_, all.x=T)
  print(as.Date(fin_mois))
  
}

colnames(defm) <- c("id_midas", liste_mois_num_d)
defm[is.na(defm)] <-0
colSums(defm[,-1])
rm(de, defm_)

#==================#
#### Table emploi ##
#==================#

mmo_22_p <- open_dataset(sprintf(paste0("%s/MMO/mmo_2022.parquet"), path_parquet))
mmo_23_p <- open_dataset(sprintf(paste0("%s/MMO/mmo_2023.parquet"), path_parquet))

mmo_22 <- mmo_22_p %>% 
  filter(id_midas %in% champ) %>% 
  select(id_midas, L_Contrat_SQN, DebutCTT, FinCTT, Nature, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>% 
  collect() 

mmo_23 <- mmo_23_p %>% 
  filter(id_midas %in% champ) %>% 
  select(id_midas, L_Contrat_SQN, DebutCTT, FinCTT, Nature, Salaire_Base, Quali_Salaire_Base, contains("emploi_bit")) %>% 
  collect() 

mmo <- bind_rows(mmo_22, mmo_23)


# Retraitements mmo
mmo <- mmo %>% 
  mutate(FinCTT = as.Date(FinCTT), DebutCTT = as.Date(DebutCTT)) %>%
  mutate(type_contrat = case_when(Nature %in% c("01", "09", "50", "82", "91") ~ "CDI",
                                  Nature %in% c("02", "07", "10", "20", "21", "32", "51", "52", "80", "81", "92", "93") ~ "CDD",
                                  Nature %in% c("03", "08") ~ "interim",
                                  Nature %in% c("29", "53", "60", "70", "89", "90") ~ "autre",
                                  TRUE ~ NA)) %>%
  mutate(ordre_contrat = case_when(type_contrat == "CDI" ~ 1,
                                   type_contrat == "CDD" ~ 2,
                                   type_contrat == "interim" ~ 3,
                                   type_contrat == "autre" ~ 4,
                                   TRUE ~ NA)) %>%
  filter(!is.na(DebutCTT)) %>%
  mutate(duree_ctt = as.numeric(FinCTT-DebutCTT)+1, 
         emploi_d = ifelse((type_contrat == "CDD" & (duree_ctt > 180 | is.na(duree_ctt))) | type_contrat == "CDI", 1, 0))
rm(mmo_22, mmo_23)
gc()



# Repérer l'emploi (durable/non_durable) ou non un jour dans le mois 
# Les contrats avec mauvaise qualité de salaire (quali_salaire_base < 7) et qui 
# ne correspondent pas à de l'emploi bit sont retirés
# L'emploi durable est défini comme CDI, fonctionnaire ou CDD de plus de 6 mois
emploi_durable <- data
emploi_non_durable <- data

for(debut_mois in debut_mois_liste){
  
  debut_mois <- as.Date(debut_mois)
  fin_mois <- debut_mois %m+% months(1) - 1
  mois <- sprintf("%02d", month(debut_mois))
  annee <- year(debut_mois)
  variable_bit <- paste0("emploi_bit_", annee, "_", mois)
  
  # les contrats dont la fin est manquante sont ceux qui sont toujours en cours
  emploi_all <- mmo %>% 
    select(id_midas,DebutCTT, FinCTT, Quali_Salaire_Base, emploi_d, emploi_bit=all_of(variable_bit)) %>%
    filter(as.Date(DebutCTT) <= fin_mois & (as.Date(FinCTT) >= debut_mois | is.na(FinCTT)) )
  
  # filtre emploi bit et qualité de salaire
  emploi_all <- emploi_all %>% 
    filter(emploi_bit==1 & Quali_Salaire_Base == 7)
  
  emploi_all <- emploi_all %>% 
    group_by(id_midas) %>% 
    filter(emploi_d == max(emploi_d)) %>% 
    distinct(id_midas, emploi_d)
  
  emploi_durable_ <- emploi_all %>% 
    filter(emploi_d==1) %>% 
    select(-emploi_d)  %>% 
    mutate(emploi_d=1) %>% 
    distinct()
  
  emploi_non_durable_ <- emploi_all %>% 
    filter(emploi_d==0) %>% 
    select(-emploi_d) %>% 
    mutate(emploi_non_d=1) %>% 
    distinct()
  
  
  colnames(emploi_durable_) <-  c("id_midas", debut_mois)
  colnames(emploi_non_durable_) <-  c("id_midas", debut_mois)
  
  emploi_durable <- merge(emploi_durable ,emploi_durable_, all.x=T)
  emploi_non_durable <- merge(emploi_non_durable ,emploi_non_durable_, all.x=T)
  
  print(as.Date(fin_mois))
  
}

colnames(emploi_durable) <- c("id_midas", liste_mois_num_d)
emploi_durable[is.na(emploi_durable)] <-0
colSums(emploi_durable[,-1])

colnames(emploi_non_durable) <- c("id_midas", liste_mois_num_d)
emploi_non_durable[is.na(emploi_non_durable)] <-0
colSums(emploi_non_durable[,-1])


traj_fd <- list(rsa=rsa,
              ass=ass,
              are=are,
              ppa=ppa,
              defm=defm,
              defm_abc=defm_abc,
              defm_de=defm_de,
              defm_de_non_ar=defm_de_non_ar,
              defm_de_ar=defm_de_ar,
              defm_a=defm_a,
              defm_bc=defm_bc,
              emploi_durable=emploi_durable,
              emploi_non_durable=emploi_non_durable,
              activite_e0=activite_e0)              

write_rds(traj_fd, sprintf(paste0("%s/traj_fd.rds"), donnees))


