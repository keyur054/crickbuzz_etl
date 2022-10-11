import pandas as pd
import requests
import logging
import time

class fetchCricketData:

  url = "https://unofficial-cricbuzz.p.rapidapi.com/matches/get-scorecard"
  querystring = {"matchId":"50921"}

  headers = {
      "X-RapidAPI-Key": "#####",
      "X-RapidAPI-Host": "unofficial-cricbuzz.p.rapidapi.com"
      }
  individual_team_score_list = []
  individual_batsman_score_list = []
  
  @staticmethod
  def appendNotExistedValue(listToAppend , key, fromListToCheck):
    if key in fromListToCheck:
      listToAppend[key] = fromListToCheck[key]
    else:
      listToAppend[key] = 0
    return listToAppend

  @staticmethod
  def convertAndSaveCSV(arrayList, name):
    df = pd.DataFrame(arrayList)
    #df.to_csv(name, index=False)
    df.to_csv(name, index = False)
    !gsutil cp name 'gs://cricbuzz_score_csv/'
    logging.info(name)

  
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
    
    self.convertAndSaveCSV(self.individual_team_score_list,'team_score.csv')
    self.convertAndSaveCSV(self.individual_batsman_score_list,'batsman_scorecard.csv')