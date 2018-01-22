# coding: utf-8

# In[1]:


import warnings

import telegram

warnings.filterwarnings('ignore')

# In[2]:


bot = telegram.Bot(token='527120092:AAFxZMzHNfk9OZYPlLmq60vPB8SIFojrm0M')

chat_id_list = [216612117]

# In[ ]:


with open("./data/pictures/last_week_top_committed_developer.txt", "r") as myfile:
    string_developer = myfile.read()
with open("./data/pictures/last_week_top_committed_repo.txt", "r") as myfile:
    string_repo = myfile.read()

# In[ ]:


for chat_id in chat_id_list:
    bot.send_photo(chat_id=chat_id, photo=open('./data/pictures/last_week_top_committed_developer.png', 'rb'))
    bot.send_photo(chat_id=chat_id, photo=open('./data/pictures/last_week_top_committed_repo.png', 'rb'))
    bot.send_photo(chat_id=chat_id, photo=open('./data/pictures/last_week_top_committed_token.png', 'rb'))
    bot.send_message(chat_id=chat_id, text="Top dev links:\n" + string_developer)
    bot.send_message(chat_id=chat_id, text="Top repo links:\n" + string_repo)


