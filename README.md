# Can ChatGPT Assist in Picking Stocks?

## Overview
This is the code for the research paper 'Can ChatGPT Assist in Picking Stocks?' by Matthias Pelster and myself, published in Finance Research Letters (https://www.sciencedirect.com/science/article/pii/S1544612323011583). We had the idea to test whether ChatGPT-4 with access to the internet is able to provide valuable investment advice and evaluate financial information in a timely manner. 

When we worked on the study, ChatGPT’s knowledge was limited to information available up until September 2021, and it wasn't able to browse the internet. As a result, it couldn't provide responses to prompts that require knowledge beyond that date. Thus, we inserted additional information from a web search engine into our prompts to overcome this limitation. While a browsing functionality was not officially supported by ChatGPT at that time, OpenAI was already working on a similar approach.

Using a live experiment, we find a positive correlation between ChatGPT-4 ratings and future earnings announcements and stock returns. We find evidence that ChatGPT-4 adjusts ratings in response to earnings surprises and news events information in a timely manner. An investment strategy based on “attractiveness ratings” by ChatGPT-4 yields positive returns.

## Methodology

I coded an application that automatizes the Chrome web browser and loads the WebChatGPT extension. WebChatGPT is a third-party tool that interfaces with search engines to obtain relevant search results, combines prompts with the information obtained from the web, and asks ChatGPT for a response based on the newly acquired information.

On a given date, the program iterates through the S&P 500 stocks and starts a conversation with ChatGPT and asks about its opinion about a particular stock's attractiveness and earnings surprise potential given the current newsflow. 

At this point, the WebChatGPT extension halts ChatGPT, opens a separate browser session in the background, and asks ChatGPT to formulate a Yahoo search query which is likely to retrieve relevant information for our prompts.

Based on this query, the program obtains the URLs of the top ten Yahoo search results within the last four weeks. For each URL, the application crawls its contents, opens separate browser sessions in the background, and asks ChatGPT to generate content summaries of the websites. 

The content summaries are then used by ChatGPT to generate attractiveness ratings and earnings surprise ratings for each stock. 

## Wouldn't it have been easier to use the GoogleAPI?

Yes, but now I can tell everyone that I am a Selenium Pro and know how to use undetected chromedriver.


