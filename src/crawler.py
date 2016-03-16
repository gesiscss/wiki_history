import os
import sys
import re
import csv
from collections import defaultdict

import dill
import wikipedia
from bs4 import BeautifulSoup
from numpy import arange, array_split

from util import data_path

out_path = os.path.join(data_path,'wiki_pages')

def extract_years(paragraphs):
    p_years  = {}
    for i,p in enumerate(paragraphs):
        years = re.findall('[^0-9](\d{4,4})[^0-9\$]',p) 
#        for y in years:
#            if validate_date(y):
#                valid_years.append(y)
        p_years[i+1] = years
    return p_years

def create_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_crawle_info():
    fpath = os.path.join(data_path,'OUT_titles_in_all_lang.txt')
    out = defaultdict(lambda:defaultdict(str))

    with open(fpath,'rb') as f: 
        concepts = f.read().strip().split('##\n')
        for c in concepts[1:]:
#            print c
            langs_title = c.strip().split('\n') 
            print langs_title[0]
            en,en_title = langs_title[0].strip().split()
            print en,'**',en_title
            out[en_title][en]=en_title
            path = os.path.join(out_path,en_title)
            create_folder(path)
            for lt in langs_title[1:]:
                temp = lt.split()
                l = temp[0]
                t = ' '.join(temp[1:])
                print l,'**',t
#                l,t = lt.strip().split()    
                out[en_title][l]=t
                path = os.path.join(out_path,en_title,l)
                create_folder(path)
                #yield l,t,path
        path = os.path.join(data_path,'OUT_titles_in_all_lang.dill') 
        dill.dump(out,open(path,'wb'))

def crawl(frange):
    path = os.path.join(data_path,'OUT_titles_in_all_lang.dill') 
    f = dill.load(open(path,'rb'))
    pages = sorted(f.keys())
#    print pages.index('History_of_Georgia_(country)')
    for i in frange:
        
        page_name = pages[i]
        print page_name
        path = os.path.join(out_path,page_name)
        create_folder(path)

        langs_title = f[page_name]
        for lang, title in langs_title.iteritems():
            print lang
            path = os.path.join(out_path,page_name,lang)
            create_folder(path)
            yield lang,title,path


def save_file(data,fpath):
    with open(fpath,'wb') as f:
        f.write(data)

if __name__ == '__main__':
    #page_names = {'en':'History_of_Germany'}
    #create_folder(out_path)
    #get_crawle_info()
    ind = int(sys.argv[1])
    indices = arange(0,193) 
    range_splites = array_split(indices,51)
    current_range = range_splites[ind]
    iterator= crawl(current_range)
#    print current_range
    pages_with_err = []
    for i in crawl(range_splites[ind]):
        lang,page_name,dir_path = i
        wikipedia.set_lang(lang)
        try:
            wiki_obj = wikipedia.page(page_name)
        except Exception as err:
            pages_with_err.append('{0}_{1}'.format(lang,page_name))
            continue 

        html = wiki_obj.html()

        soup = BeautifulSoup(html,'lxml')
        html = soup.prettify("utf-8")
        path = os.path.join(dir_path, '{0}.html'.format(lang))
        save_file(html,path)

        paragraphs = [p.getText() for p in soup.find_all('p')]
        txt = '====\n'.join(paragraphs)
        path = os.path.join(dir_path, 'paragraphs.txt')
        save_file(txt.encode('utf-8'),path)

        #meta data
        try:
            cat = wiki_obj.categories
            cat = '\n'.join(cat)
            path = os.path.join(dir_path, 'categories.txt')
            save_file(cat.encode('utf-8'),path)
        except Exception as err:
            path = os.path.join(dir_path, 'categories.txt')
            save_file(str(err),path)
            pass
        try:
            links = wiki_obj.references
            links = '\n'.join(links)
            path = os.path.join(dir_path, 'references.txt')
            save_file(links,path)
        except Exception as err:
            path = os.path.join(dir_path, 'references.txt')
            save_file(str(err),path)
            pass
        rev_id = wiki_obj.revision_id
        parent_id = wiki_obj.parent_id
        out  = 'rev:{0}\nparent_id:{1}'.format(rev_id,parent_id)
        path = os.path.join(dir_path, 'rev_ids.txt')
        save_file(out,path)
    path = os.path.join(data_path,'err_{0}'.format(ind))
    dill.dump(pages_with_err,open(path,'wb'))
#        sys.exit()

#        r = extract_years(paragraphs)

#        with open('german_history_4digits.csv','wb') as f:
#            writer = csv.writer(f)
#            writer.write(('paragraph number','numbers'))
#            for i,y in r.iteritems():
#                writer.write((i,y))
