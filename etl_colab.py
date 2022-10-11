

import requests
import pandas as pd

from google.colab import auth
auth.authenticate_user()

project_id = 'cricbuzz-score'
!gcloud config set project {project_id}

class fetchCricketData:

  # url for the api and match id which will be used to fetch the specific
  # match's score.

  url = "https://unofficial-cricbuzz.p.rapidapi.com/matches/get-scorecard"
  querystring = {"matchId":"50921"}

  headers = {
      "X-RapidAPI-Key": "88f6f91fd0msh4e8e0f86448e525p181c08jsnc3ff883e206e",
      "X-RapidAPI-Host": "unofficial-cricbuzz.p.rapidapi.com"
      }

  individual_team_score_list = []
  individual_batsman_score_list = []

   # This global function will appned 0 if any json valus is missing.
  @staticmethod
  def appendNotExistedValue(listToAppend , key, fromListToCheck):
    if key in fromListToCheck:
      listToAppend[key] = fromListToCheck[key]
    else:
      listToAppend[key] = 0
    return listToAppend

  # def convertAndSaveCSV(arrayList, name):
  #   df = pd.DataFrame(arrayList)
  #   #df.to_csv(name, index=False)
  #   df.to_csv(name, index = False)
  #   print(name)
  #   !gsutil cp "'"+ name + "'"  gs://cricbuzz_score_csv//
  #   logging.info(name)

  
  @classmethod
  def requestandConvertApiResponse(self):

    response = requests.request("GET", self.url, headers=self.headers, params=self.querystring)

    score = response.json()

    #Iterating over the each team's inning's stats
    for scorecard_team in score['scorecard']:

      #Create array for the individual team
      individual_score = { 'runs' : scorecard_team['score'],
                          'wickets' :scorecard_team['wickets'],
                          'overs' :scorecard_team['overs'],
                          'runRate' :scorecard_team['runRate'],
                          'team' :scorecard_team['batTeamSName'],
                          'teamL' :scorecard_team['batTeamName'],
                          'inningsOrder' :scorecard_team['inningsId']
                          }

      self.individual_team_score_list.append(individual_score)

      #Iterating in each batsman scorecard
      for batsaman_performance in scorecard_team['batsman']:
        batsman_score_list = { 
            'name' :batsaman_performance['name'],
            'strikeRate' :batsaman_performance['strkRate'],
            'team' : scorecard_team['batTeamSName']
            }

        batsman_score_list = self.appendNotExistedValue(batsman_score_list, 'runs', batsaman_performance)
        batsman_score_list = self.appendNotExistedValue(batsman_score_list, 'balls', batsaman_performance)
        batsman_score_list = self.appendNotExistedValue(batsman_score_list, 'fours', batsaman_performance)
        batsman_score_list = self.appendNotExistedValue(batsman_score_list, 'sixes', batsaman_performance)
        batsman_score_list = self.appendNotExistedValue(batsman_score_list, 'outDec', batsaman_performance)

        self.individual_batsman_score_list.append(batsman_score_list)
    
    #self.convertAndSaveCSV(self.individual_team_score_list,'team_score.csv')
    #self.convertAndSaveCSV(self.individual_batsman_score_list,'batsman_scorecard.csv')
    # Saving csv to cloud storage
    team_score_df = pd.DataFrame(self.individual_team_score_list)
    team_score_df.to_csv('team_score.csv', index=False)
    !gsutil cp team_score.csv  gs://cricbuzz_score_csv//

    indi_score_df = pd.DataFrame(self.individual_batsman_score_list)
    indi_score_df.to_csv('batsman_scorecard.csv', index=False)
    !gsutil cp batsman_scorecard.csv  gs://cricbuzz_score_csv//

fetchCricketData.requestandConvertApiResponse()