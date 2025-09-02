Ce dossier fournit les programmes ayant servi à produire les publications de la Dares suivantes, issues de la collection Dares Focus :

- DF n° 54 de la Dares : "Bénéficiaires du RSA et inscription à France Travail : profil et situation" [https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail-profil-et-situation]
- DF n°58 de la Dares : "Bénéficiaires du RSA et inscription à France travail : parcours sur une année" [https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail-parcours-sur-une-annee]
- DF n°38 de la Dares : "Bénéficiaires du RSA et inscription à France Travail : parcours à horizon d'un an" [https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail]

Source des données : Midas, 2021-2022-2023.

Les programmes sont exécutés pour la dernière fois en septembre 2025 avec le logiciel R version 4.5.1.

Les scripts utilisés sont les suivants : 
- 00_Creation_base.R : création de la base
- 01_DF1_profil_et_situation.R : caractérisation des bénéficiaires du RSA selon qu'ils sont inscrits ou non à France Travail en juin 2022.
- 02_DF2_parcours_sur_une_annee.R : situation passée des bénéficiaires du RSA vis-à-vis de l'emploi, des minima sociaux et du chômage, selon leur inscription à France Travail en juin 2022.
- 02_DF3_Retouremploibit.R : création d'indicateurs de retour en emploi BIT à horizon 1 an pour les bénéficiaires du RSA de juin 2022 en fonction de leur inscription à France Travail à cette date et de leurs caractéristiques socio-démographiques.
- 03_DF3_Trajectoirescroisees.R : création d'indicateurs de la situation croisée au regard du RSA, de l'inscription à France Travail et de l'emploi BIT à horizon 1 an, en fonction de la situation en juin 2022. 
