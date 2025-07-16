Ce dossier fournit les programmes ayant servi à produire une partie du Dares Focus n°XX de la Dares : "Bénéficiaires du RSA et inscription à France Travail : parcours à horizon d'un an" [https://dares.travail-emploi.gouv.fr/publication/beneficiaires-du-rsa-et-inscription-france-travail]

Source des données : Midas, 2022-2023.

Les programmes sont exécutés pour la dernière fois en juillet 2025 avec le logiciel R version 4.5.1.

Les scripts utilisés sont les suivants : 
- 01_CodeDF3_Creationbase.R : création de la base
- 02_CodeDF3_Retouremploibit.R : création d'indicateurs de retour en emploi BIT à horizon 1 an pour les bénéficiaires du RSA de juin 2022 en fonction de leur inscription à France Travail à cette date et de leurs caractéristiques socio-démographiques.
- 03_CodeDF3_Trajectoirescroisees.R : création d'indicateurs de la situation croisée au regard du RSA, de l'inscription à France Travail et de l'emploi BIT à horizon 1 an, en fonction de la situation en juin 2022. 
