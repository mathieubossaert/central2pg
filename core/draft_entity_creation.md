# Projet de fonction de création d'entité

## doc : 
  * https://docs.getodk.org/central-api-entity-management/
  * https://reqbin.com/req/c-fypvcnti/curl-json-request-example
  
## Idée
  Une table dédiée avec : 
  * projetct_id, 
  * nom du dataset, 
  * uuid de l'entité, 
  * label, 
  * data (json), dont geometrie en 4326 au format ODK 
  * created (date)

Tache cron qui appelle la fonction à intervalle régulier avec regroupement des entités à créer (created null ou false) par projet et dataset


