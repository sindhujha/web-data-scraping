from __future__ import unicode_literals
import csv
import os, sys
from scrapy import item
from scrapy.spiders import BaseSpider
from scrapy.selector import Selector
from scrapy.utils import console
from urlparse import urlparse
from scraper.items import ScraperItem
from scrapy.http.request import Request
from scrapy.log import *
import math

reload(sys)
sys.setdefaultencoding("UTF-8")
class MySpider(BaseSpider):
    def closed(self, reason):
        tf = open("temp_user_file.csv", "wb")
        temp = csv.writer(tf,dialect='excel')
        temp.writerow(["Username","Location","Employer","Bio","Joining Date","Design Quotient-research points","Design Quotient-idea points",
                    "Design Quotient-evaluation points","Design Quotient-collaboration points","Design Quotient-total points","Contributions at Stage 1","Contributions at Stage 2",
                       "Contributions at Stage 3","Contributions at Stage 4"])
        for key in self.author_dict:
            temp.writerow([key,self.author_dict[key]["location"],self.author_dict[key]["profession"],self.author_dict[key]["bio"],self.author_dict[key]["Date of Joining OpenIdeo"],self.author_dict[key]["Research"],self.author_dict[key]["Idea"],
                  self.author_dict[key]["Evaluation"],self.author_dict[key]["Collaboration"],self.author_dict[key]["Total"],self.author_dict[key]["Contribution_count_1"],
                          self.author_dict[key]["Contribution_count_2"],self.author_dict[key]["Contribution_count_3"],self.author_dict[key]["Contribution_count_4"]])

    def __init__(self):
        self.author_dict = {}
    name = "openid"
    allowed_domains = ["challenges.openideo.com"]
    start_urls = ["https://challenges.openideo.com/challenge/"]
    baseurl="https://challenges.openideo.com"
    def parse(self, response):
        baseurl="https://challenges.openideo.com"
        uf = open("user_file.csv", "wb")
        c = csv.writer(uf,dialect='excel')
        c.writerow(["Username","Location","Employer","Bio","Joining Date","Design Quotient-research points","Design Quotient-idea points",
                    "Design Quotient-evaluation points","Design Quotient-collaboration points","Design Quotient-total points"])
        nf = open("network_file.csv", "wb")
        network_csv = csv.writer(nf,dialect='excel')
        network_csv.writerow(["Team name","Team Members","Project ID","Project_Stage","Team Leader","Team Size"])
        pf = open("project_file.csv", "wb")
        proj_csv = csv.writer(pf,dialect='excel')
        proj_csv.writerow(["Project Id","Project Name","Project Description","# of contributions at stage1"])
        hxs = Selector(response)
        completed_project_list = hxs.xpath("//div[@class='details-box distance-padding-top distance-padding-bottom col-8']//h2[@class='challenge-title sub-headline-text']/a")
        for projects_no in range (0,len(completed_project_list)):
            proj_name = completed_project_list[projects_no].select("text()").extract()[0].encode('utf-8').strip()
            proj_url = completed_project_list[projects_no].xpath("@href").extract()[0].encode('utf-8').strip()
            yield Request(url= (baseurl+proj_url),meta={'proj_id':projects_no,'proj_name':proj_name,'csv':c,'proj_csv':proj_csv,'network_csv':network_csv}, callback = self.parse_stage)





    def parse_stage(self, response):
        item = ScraperItem()
        baseurl="https://challenges.openideo.com"
        if 'proj_name' in response.request.meta.keys():
            proj_name=response.request.meta['proj_name']
        if 'proj_id' in response.request.meta.keys():
            proj_id=response.request.meta['proj_id']
        if 'csv' in response.request.meta.keys():
            c=response.request.meta['csv']
        if 'network_csv' in response.request.meta.keys():
            network_csv=response.request.meta['network_csv']
        if 'proj_csv' in response.request.meta.keys():
            proj_csv=response.request.meta['proj_csv']
        hxs = Selector(response)
        phase_name = hxs.xpath("//a[@class='phase-name indent-text']")
        phase_caption = hxs.xpath("//span[@class='phase-caption indent-text']")
        proj_brief_url = hxs.xpath("//p[@class='distance-margin-bottom']/a/@href").extract()[0].strip()
        csv_contribution=""
        for i in range(0,len(phase_name)):
            phase= str(phase_name[i].xpath("text()").extract()[0])
            phase=str((phase).strip())
            contribs = str(phase_caption[i].xpath("text()").extract()[0]).strip()
            phase_link= str(phase_name[i].xpath("@href").extract()[0]).strip()
            csv_contribution += phase+": "+contribs+" "
            if phase == "Research" or phase == "Inspiration":
                stage_count = 1;
            elif (phase == "Ideas" or phase == "Concepting" or phase == "Agenda Concepts" or phase == "Concept"):
                stage_count = 2;
            elif phase == "Refinement" or phase == "Refine" or phase == "Prototyping" :
                stage_count = 3;
            elif phase == "Winning Ideas" or phase == "Top Ideas" or phase == "HIGHLIGHTS" or phase == "Winners" or phase == "Shortlist" or \
                            phase == "Winners Announced" or phase == "Winner Announced" or phase == "Winning Concepts" or phase == " Agenda announced!":
                stage_count = 4;
            else:
                stage_count = 0;
            yield Request(url= (baseurl+phase_link),meta={'item':item,'phase':phase,'proj_id':proj_id,'proj_name':proj_name,'csv':c,'proj_csv':proj_csv,'network_csv':network_csv,'stage_count':stage_count}, callback = self.parse_url)
        yield Request(url= (baseurl+proj_brief_url),meta={'proj_id':proj_id,'item':item,'phase':phase,'proj_name':proj_name,'csv':c,
                                                          'proj_csv':proj_csv,'csv_contribution':csv_contribution}, callback = self.parse_url)



    def parse_url(self, response):
        hxs = Selector(response)
        item = response.request.meta['item']
        if 'item' in response.request.meta.keys():
             item = response.request.meta['item']
        if 'phase' in response.request.meta.keys():
             phase = response.request.meta['phase']
        if 'author_dict' in response.request.meta.keys():
            author_dict = response.request.meta['author_dict']
        phase = response.request.meta['phase']
        if 'proj_name' in response.request.meta.keys():
            proj_name = response.request.meta['proj_name']
        if 'proj_id' in response.request.meta.keys():
            proj_id=response.request.meta['proj_id']
        if 'csv' in response.request.meta.keys():
            c=response.request.meta['csv']
        if 'proj_csv' in response.request.meta.keys():
            proj_csv=response.request.meta['proj_csv']
        if 'network_csv' in response.request.meta.keys():
            network_csv=response.request.meta['network_csv']
        if 'csv_contribution' in response.request.meta.keys():
            csv_contribution=response.request.meta['csv_contribution']
        if 'stage_count' in response.request.meta.keys():
            stage_count=response.request.meta['stage_count']
        overview=hxs.xpath("//section[@itemprop='articleBody']//p/text()|//section[@itemprop='articleBody']/text() | //section[@itemprop='articleBody']//div/text()").extract()
        if overview:
            strover = ""
            for i in range(0,len(overview)):
                strover += overview[i].encode('utf-8').strip()
            proj_csv.writerow([proj_id,proj_name,strover,csv_contribution])
        contribution_name = hxs.xpath("//article[@class='js-contribution-list-item col-4 clear-fix ']|//article[@class='js-contribution-list-item col-4 clear-fix last-item']")
        if contribution_name:
            for i in range(0,len(contribution_name)):
                name = contribution_name[i].xpath(".//div[@class='listing-text-content distance-margin-bottom']/h1/a/text()").extract()[0].encode('utf-8').strip()
                author = contribution_name[i].xpath(".//section[@class='author-box-small user-box clear-fix']//div[@class='details']/h1/a/text()").extract()[0].encode('utf-8').strip()

                if author not in self.author_dict.keys():
                    self.author_dict[author]={}
                    zero = 0
                    self.author_dict[author]={'Contribution_count_1':zero,'Contribution_count_2':zero,'Contribution_count_3':zero,'Contribution_count_4':zero}
                    temp = 1
                    if stage_count == 1:
                        self.author_dict[author]={'Contribution_count_1':temp,'Contribution_count_2':zero,'Contribution_count_3':zero,'Contribution_count_4':zero}
                    elif stage_count==2:
                        self.author_dict[author]={'Contribution_count_2':temp,'Contribution_count_3':zero,'Contribution_count_4':zero,'Contribution_count_1':zero}
                    elif stage_count==3:
                        self.author_dict[author]={'Contribution_count_3':temp,'Contribution_count_4':zero,'Contribution_count_1':zero,'Contribution_count_2':zero}
                    elif stage_count==4:
                        self.author_dict[author]={'Contribution_count_4':temp,'Contribution_count_1':zero,'Contribution_count_2':zero,'Contribution_count_3':zero}

                else:
                    if stage_count==1:
                        counts1 = int((self.author_dict[author]["Contribution_count_1"]))
                        self.author_dict[author]["Contribution_count_1"] = counts1 + 1
                    elif stage_count==2:
                        counts2 = int((self.author_dict[author]["Contribution_count_2"]))
                        self.author_dict[author]["Contribution_count_2"] = counts2 + 1
                    elif stage_count==3:
                        counts3 = int((self.author_dict[author]["Contribution_count_3"]))
                        self.author_dict[author]["Contribution_count_3"] = counts3 + 1
                    elif stage_count==4:
                        counts4 = int((self.author_dict[author]["Contribution_count_4"]))
                        self.author_dict[author]["Contribution_count_4"] = counts4 + 1


                comment = str(contribution_name[i].xpath(".//section[@class='author-box-small user-box clear-fix']//a[@class='comment-link']/text()").extract()[0]).strip()
                author_profile = contribution_name[i].xpath(".//section[@class='author-box-small user-box clear-fix']//div[@class='details']/h1/a/@href").extract()[0].strip()
                yield Request(url= ("https://challenges.openideo.com"+author_profile),meta={'phase':phase,'proj_id':proj_id,'author':author,'csv':c,'network_csv':network_csv,'flag':1}, callback = self.parse_author)

            pages_count = hxs.xpath("//span[@class='js-page-count']/text()").extract()
            paginator = hxs.xpath("//div[@class=' paginator']/@data-paginator-size").extract()
            if paginator:
                paginator = int(hxs.xpath("//div[@class=' paginator']/@data-paginator-size").extract()[0].strip())
            if not pages_count:
                pages_count = len(hxs.xpath("//div[@class='clear-fix boxes']/a"))
            else:
                pages_count = int(str(pages_count[0]).strip())
            if (pages_count != 0):
                count=abs(pages_count-paginator)
                page_list = hxs.xpath("//div[@class='clear-fix boxes']/a")
                curr_page = int(hxs.xpath("//div[@class=' paginator']/@data-active-index").extract()[0].strip())
                if (curr_page < pages_count):
                    if (curr_page >= count+1):
                        next_page = page_list[(curr_page+1)-(count+1)].xpath("./span/text()").extract()[0].strip()
                        next_page_url = page_list[(curr_page+1)-(count+1)].xpath("./@href").extract()[0].strip()
                    else:
                        next_page = page_list[1].xpath("./span/text()").extract()[0].strip()
                        next_page_url = page_list[1].xpath("./@href").extract()[0].strip()
                    yield Request(url= ("https://challenges.openideo.com"+next_page_url),meta={'proj_id':proj_id,'item':item,'phase':phase,'csv':c,'proj_name':proj_name,'proj_csv':proj_csv,'network_csv':network_csv,'stage_count':stage_count}, callback = self.parse_url)


    def parse_author(self, response):

        c=response.request.meta['csv']
        if 'network_csv' in response.request.meta.keys():
            network_csv=response.request.meta['network_csv']
        if 'team_name' in response.request.meta.keys():
            team_name=response.request.meta['team_name']
        if 'phase' in response.request.meta.keys():
             phase = response.request.meta['phase']
        if 'proj_id' in response.request.meta.keys():
            proj_id=response.request.meta['proj_id']
        if 'contribution_count' in response.request.meta.keys():
            contribution_count=response.request.meta['contribution_count']
        flag = response.request.meta['flag']
        author = response.request.meta['author']
        hxs = Selector(response)
        if (flag == 1):
            country = hxs.xpath("//article[@class='profile']/div[@class='user-info row']//div[@class='distance-padding-bottom distance-margin-bottom bottom-separator']/p[@class='secondary-text country']/text()").extract()
            profession = hxs.xpath("//article[@class='profile']/div[@class='user-info row']//div[@class='distance-padding-bottom distance-margin-bottom bottom-separator']/p[@class='secondary-text company']/text()").extract()
            description = hxs.xpath("//article[@class='profile']/div[@class='user-info row']//div[@class='distance-padding-bottom distance-margin-bottom bottom-separator']/p[@class='primary-text distance-margin-top']/text()").extract()
            joining_date = hxs.xpath("//div[@class='platform-font distance-margin-top distance-margin-bottom']/time/text()").extract()[0].encode('utf-8').strip()
            design_quotient = hxs.xpath("//section[@class='design-quotient platform-font']//div[@class='data-elements primary-text distance-margin-top distance-padding-right data-visible']")
            design_arr = design_quotient.xpath("./div")
            place =hxs.xpath("//div[@class='dynamic-values secondary-text']//p")
            location =""
            if country:
                location += "Country: "+country[0].encode('utf-8').strip()
                self.author_dict[author]['location']=location
                if place:
                    for i in range(0,len(place)):
                        if(place[i].xpath("./span/text()").extract()[0].strip() == "City:"):
                            location += " City: "+(place[i].xpath("text()").extract()[1].strip())
                        elif(place[i].xpath("./span/text()").extract()[0].strip() == "State:"):
                            location += " State: "+(place[i].xpath("text()").extract()[1].strip())
                    self.author_dict[author]['location']=location
            elif place:
                for i in range(0,len(place)):
                        if(place[i].xpath("./span/text()").extract()[0].strip() == "City:"):
                            location += " City: "+(place[i].xpath("text()").extract()[1].strip())
                        elif(place[i].xpath("./span/text()").extract()[0].strip() == "State:"):
                            location += " State: "+(place[i].xpath("text()").extract()[1].strip())
                self.author_dict[author]['location']=location

            else:
                self.author_dict[author]['location']='N/A'


            if profession:
                profession = profession[0].encode('utf-8').strip()
                self.author_dict[author]['profession']=profession
            else:
                self.author_dict[author]['profession']='N/A'

            bio=""
            if description:
                bio = description[0].encode('utf-8').strip()
                self.author_dict[author]['bio']=bio
            else:
                self.author_dict[author]['bio']='N/A'
            self.author_dict[author]['Date of Joining OpenIdeo']=joining_date
            if design_arr:
                for count in range(0,len(design_arr)):
                    key_name = design_arr[count].xpath("./p[@class='name']/text()").extract()[0].encode('utf-8').strip()
                    value_name = design_arr[count].xpath("./p[@class='value']/text()").extract()[0].encode('utf-8').strip()
                    self.author_dict[author][key_name] = value_name
                c.writerow([author,location,profession,bio,joining_date,self.author_dict[author]["Research"],self.author_dict[author]["Idea"],
                    self.author_dict[author]["Evaluation"],self.author_dict[author]["Collaboration"],self.author_dict[author]["Total"]])
            else:
                self.author_dict[author]["Research"]=0
                self.author_dict[author]["Idea"]=0
                self.author_dict[author]["Evaluation"]=0
                self.author_dict[author]["Collaboration"]=0
                self.author_dict[author]["Total"]=0
                c.writerow([author,location,profession,description,joining_date,"0","0","0","0","0"])
            team_url = hxs.xpath("//section[@id='team-membership']/div/a/@href").extract()
            if team_url:
                team_url = team_url[0].strip()
                yield Request(url= ("https://challenges.openideo.com"+team_url),
                              meta={'proj_id':proj_id,'phase':phase,'author':author,'csv':c,'network_csv':network_csv,'flag':2}, callback = self.parse_author)
        elif (flag == 2):
            team_member_list = hxs.xpath("//section[@class='listing contribution-list team-membership-list']/div//article \
        [@class='js-contribution-list-item col-4 clear-fix']/div[@class='main-item-info clear-fix distance-margin-bottom']//div[@class='listing-details']/div/h1/a")
            if team_member_list:
                for teams in range(0,len(team_member_list)):
                    team_link = team_member_list[teams].xpath("@href").extract()[0].strip()
                    team_name  = team_member_list[teams].xpath("text()").extract()[0].encode('utf-8').strip()
                    yield Request(url= ("https://challenges.openideo.com"+team_link),
                              meta={'proj_id':proj_id,'phase':phase,'author':author,'csv':c,'network_csv':network_csv,'flag':3,'team_name':team_name}, callback = self.parse_author)
        else:
            member_list = hxs.xpath("//section[@class='col-4 clear-fix team-box-expanded user-box']/div[@class='details secondary-text']/a/text()").extract()
            leader = hxs.xpath("//section[@class='author-box-big distance-padding-top distance-padding-bottom row distance-margin-bottom']//div[@class='details']/h1[@class='secondary-text']/a/text()").extract()[0].encode('utf-8').strip()
            for members in range(0,len(member_list)):
                network_csv.writerow([team_name,member_list[members].encode('utf-8').strip(),proj_id,phase,leader,len(member_list)])

