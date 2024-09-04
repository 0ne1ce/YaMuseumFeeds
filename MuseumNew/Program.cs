using System;
using System.Text.Json;
using System.Xml;
using System.Xml.Linq;
using System.IO;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace Function;
public class Handler
{
    private const string bucketName = "yandex-museum";
    private const string googleKeyName = "feeds/google/google_merchant_center_feed.xml";
    private const string yandexKeyName = "feeds/yandex/yandex_products_feed.xml";
    static string requestedData = "";

    private static AmazonS3Config configS3 = new AmazonS3Config
    {
        ServiceURL = "https://s3.yandexcloud.net"
    };

    private static AmazonS3Client client;
    
    /// <summary>
    /// Handler for Serverless Function in Yandex Cloud.
    /// </summary>
    /// <param name="i">optional string input</param>
    public void FunctionHandler(string i) {
        Main();
    }

    /// <summary>
    /// Starting process of creating connection with Object Storage and objects in it.
    /// </summary>
    public static void Main()
    {
        client = new AmazonS3Client(configS3);
        WritingAnObjectAsync().Wait();
    }
    
    /// <summary>
    /// Writing objects in Object Storage.
    /// </summary>
    static async Task WritingAnObjectAsync()
    {
        try
        {
            int categoryAdditionalId = 100;
            Dictionary<string, string> categoriesDictionary = new Dictionary<string, string>();

            // Getting uploaded feed object.
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = "feeds/feed.json"
            };
            
            using (GetObjectResponse response = await client.GetObjectAsync(request))
            using (Stream responseStream = response.ResponseStream)
            using (StreamReader reader = new StreamReader(responseStream))
            {
                string title = response.Metadata["x-amz-meta-title"];
                string contentType = response.Headers["Content-Type"];

                requestedData = reader.ReadToEnd();
            }

            // Creating Google feed in .xml format.
            var putRequestGoogle = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = googleKeyName,
                ContentBody = GoogleFeedCreationProcess(requestedData)
            };
            PutObjectResponse responseGoogle = await client.PutObjectAsync(putRequestGoogle);

            // Creating Yandex feed in xml. format.
            var putRequestYandex = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = yandexKeyName,
                ContentBody = YandexFeedCreationProcess(requestedData, ref categoryAdditionalId,
                    categoriesDictionary)
            };
            PutObjectResponse responseYandex = await client.PutObjectAsync(putRequestYandex);
        }
        // Object storage related errors.
        catch (AmazonS3Exception e)
        {
            Console.WriteLine(e.Message);
        }
        // Other unpredicted errors.
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    /// <summary>
    /// Process of creating Google feed.
    /// </summary>
    /// <param name="data">feed.json file data.</param>
    /// <returns>data of Google feed .xml document in string format.</returns>
    public static string GoogleFeedCreationProcess(string data)
    {
        // Creating .xml document for feed.
        XmlDocument googleFeed = new XmlDocument();
        string namespaceUri = "http://base.google.com/ns/1.0";
        string version = "2.0";
        
        // Creating and adding essential elements and attributes for Google feed.
        XmlElement rss = googleFeed.CreateElement("rss");
        rss.SetAttribute("xmlns:g", namespaceUri);
        rss.SetAttribute("version", version);
        
        googleFeed.AppendChild(rss);
            
        XmlElement channelElem = googleFeed.CreateElement("channel");
        
        // Creating elements which present in every Google feed.
        GoogleCreateMainElements(ref googleFeed, ref rss, ref channelElem);

        // Parsing data for our future feed from feed.json file.
        using var jsonFile = JsonDocument.Parse(data);
        JsonElement jsonRoot = jsonFile.RootElement;
        
        // Dictionary with pairs that contains json element as a key and required element for Google feed as a value.
        // Helps in creating different elements for every item in GoogleCreateTypeElement().
        Dictionary<string, string> ElementTextValues = new Dictionary<string, string>()
        {
            {"id", "offerId"},
            {"title", "title"},
            {"description", "description"},
            {"image_link", "url"},
            {"additional_image_link", "url"},
            {"availability", "isVisible"},
            {"price", "price"},
            {"brand", "vendor"}
        };
        // Loop that are used to create items with their essential elements in google feed.
        for (int i = 0; i < jsonRoot.GetArrayLength(); i++)
        {
            XmlElement itemElem = googleFeed.CreateElement("item");
            
            JsonElement item = jsonRoot[i];
                
            string? idTextJson = item.GetProperty("offerId").ToString();
            // Creating id, title and description element for item.
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "id", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "title", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "description", namespaceUri);
            
            // Creating link element for item.
            XmlElement linkElem = googleFeed.CreateElement("g", "link", namespaceUri);
            XmlText linkText = googleFeed.CreateTextNode($@"https://museum.yandex.ru/product/{idTextJson}");
            linkElem.AppendChild(linkText);
            itemElem.AppendChild(linkElem);
            
            // Creating links of images as elements and adding them in item.
            XmlElement imageLinkElem = googleFeed.CreateElement("g", "image_link", namespaceUri);
            JsonElement imageLinkArray = item.GetProperty("pictures");
            
            JsonElement firstPicJson = imageLinkArray[0];
            GoogleCreateTypeElement(ref itemElem, ref firstPicJson, ElementTextValues, ref googleFeed, "image_link", namespaceUri);
            
            for (int j = 1; j < imageLinkArray.GetArrayLength(); j++)
            {
                JsonElement picJson = imageLinkArray[j];
                
                GoogleCreateTypeElement(ref itemElem,ref picJson, ElementTextValues, ref googleFeed, "additional_image_link", namespaceUri);
            }

            // Creating condition element for item.
            XmlElement conditionElem = googleFeed.CreateElement("g", "condition", namespaceUri);
            XmlText conditionText = googleFeed.CreateTextNode("новый");
            
            conditionElem.AppendChild(conditionText);
            itemElem.AppendChild(conditionElem);
            
            // Getting currency from feed.json to use in proper creation of price element.
            JsonElement price = item.GetProperty("price");
            string? currency = (price.GetProperty("currencyId").ToString() == "RUR") ? "RUB" : price.GetProperty("currencyId").ToString();
            
            // Creating availability, price and brand elements of item.
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "availability", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "price", namespaceUri, currency);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "brand", namespaceUri);
            
            // Adding item to Google feed .xml document.
            channelElem.AppendChild(itemElem);
            // Adding elements to the root of .xml document. 
            rss?.AppendChild(channelElem);
        }
        Console.WriteLine("File saved successfully!");

        return googleFeed.OuterXml;
    }
    /// <summary>
    /// Creating elements which present in every Google feed.
    /// </summary>
    /// <param name="feed">feed in .xml format.</param>
    /// <param name="root">root of .xml document of feed.</param>
    /// <param name="channel">channel element from .xml document.</param>
    public static void GoogleCreateMainElements(ref XmlDocument feed, ref XmlElement? root, ref XmlElement channel)
    {
        // Creating title, link and description elements in feed.
        XmlElement mainTitleElem = feed.CreateElement("title");
        XmlText mainTitleText = feed.CreateTextNode("Яндекс Музей");
        mainTitleElem.AppendChild(mainTitleText);
        
        XmlElement mainLinkElem = feed.CreateElement("link");
        XmlText mainLinkText = feed.CreateTextNode("https://museum.yandex.ru");
        mainLinkElem.AppendChild(mainLinkText);
            
        XmlElement mainDescriptionElem = feed.CreateElement("description");
        XmlText mainDescriptionText = feed.CreateTextNode("Фид Яндекс Музея");
        mainDescriptionElem.AppendChild(mainDescriptionText);
        
        // Adding elements in feed.
        channel.AppendChild(mainTitleElem);
        channel.AppendChild(mainLinkElem);
        channel.AppendChild(mainDescriptionElem);

        root?.AppendChild(channel);
    }
    /// <summary>
    /// Process of creating Yandex feed.
    /// </summary>
    /// <param name="data">feed.json file data.</param>
    /// <param name="categoryAdditionalId">additional id for each category in feed.</param>
    /// <param name="categoriesDictionary">dictionary with pairs where category name is a key and category id
    /// is a value.</param>
    /// <returns>data of Yandex feed .xml document in string format.</returns>
    public static string YandexFeedCreationProcess(string data, ref int categoryAdditionalId,
        Dictionary<string, string> categoriesDictionary)
    {
        // Creating .xml document for feed.
        XmlDocument yandexFeed = new XmlDocument();

        // Creating and adding essential elements and attributes for Google feed.
        XmlElement yml_catalog = yandexFeed.CreateElement("yml_catalog");
        DateTime date1 = DateTime.Now;
        yml_catalog.SetAttribute("date", date1.ToString());
        yandexFeed.AppendChild(yml_catalog);
        
        XmlElement shop = yandexFeed.CreateElement("shop");
        
        XmlElement categories = YandexCreateMainElements(ref yandexFeed, ref yml_catalog, ref shop);

        XmlElement offers = yandexFeed.CreateElement("offers");
        
        // Parsing data for our future feed from feed.json file.
        using var jsonFile = JsonDocument.Parse(data);
        JsonElement jsonRoot = jsonFile.RootElement;
        
        // Dictionary with pairs that contains json element as a key and required element for Google feed as a value.
        // Helps in creating different elements for every item in YandexCreateTypeElement().
        Dictionary<string, string> ElementTextValues = new Dictionary<string, string>()
        {
            {"name", "title"},
            {"price", "price"},
            {"currencyId", "price"},
            {"vendor", "vendor"},
            {"picture", "url"},
            {"description", "description"}
        };
        
        // Loop that are used to create offers with their essential elements in yandex feed.
        for (int i = 0; i < jsonRoot.GetArrayLength(); i++)
        {
            XmlElement offerElem = yandexFeed.CreateElement("offer");
            JsonElement item = jsonRoot[i];
            
            string? idTextJson = item.GetProperty("offerId").ToString();
            offerElem.SetAttribute("id", idTextJson);

            YandexCreateTypeElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "name");
            
            XmlElement urlElem = yandexFeed.CreateElement("url");
            XmlText urlText = yandexFeed.CreateTextNode($@"https://museum.yandex.ru/{idTextJson}");
            urlElem.AppendChild(urlText);
            offerElem.AppendChild(urlElem);
            
            // Creating price and currencyId elements.
            YandexCreatePriceElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "price");
            YandexCreatePriceElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "currencyId");
            
            // Checks if category of current offer exists.
            string? otherCategory = item.GetProperty("otherCategory").ToString().Trim();
            if (otherCategory == null || otherCategory.Length < 1)
            {
                otherCategory = "Другое";
            }
            if (!categoriesDictionary.ContainsKey(otherCategory))
            {
                categoriesDictionary.Add(otherCategory, categoryAdditionalId.ToString());
                categoryAdditionalId++;
            }
            
            // Creating categoryId element using dictionary of categories.
            XmlElement categoryId = yandexFeed.CreateElement("categoryId");
            XmlText categoryIdText = yandexFeed.CreateTextNode(categoriesDictionary[otherCategory]);
            categoryId.AppendChild(categoryIdText);
            offerElem.AppendChild(categoryId);
            
            YandexCreateTypeElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "vendor");
            // Creating links of pictures elements.
            JsonElement imageLinkArray = item.GetProperty("pictures");
            for (int j = 0; j < imageLinkArray.GetArrayLength(); j++)
            {
                JsonElement picJson = imageLinkArray[j];

                YandexCreateTypeElement(ref offerElem, ref picJson, ElementTextValues, ref yandexFeed, "picture");
            }
            // Creating description element.
            YandexCreateTypeElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "description");
            // Adding item to Yandex feed .xml document.
            offers.AppendChild(offerElem);
        }
        // Creating categories, their proper attributes and adding them as elements.
        foreach (var element in categoriesDictionary)
        {
            XmlText categoryText = yandexFeed.CreateTextNode(element.Key);
            XmlElement category = yandexFeed.CreateElement("category");
            category.SetAttribute("id", element.Value);
            category.SetAttribute("parentId", "1");
            category.AppendChild(categoryText);
            categories.AppendChild(category);
        }
        // Adding our created offers. 
        shop.AppendChild(offers);

        // Adding elements to the root of .xml document. 
        yml_catalog.AppendChild(shop);
        
        Console.WriteLine("File saved successfully!");

        return yandexFeed.OuterXml;
    }
    
    /// <summary>
    /// Creating elements which present in every Yandex feed.
    /// </summary>
    /// <param name="feed">feed.json file data.</param>
    /// <param name="root">root of .xml document of feed.</param>
    /// <param name="shop">shop element from .xml document.</param>
    /// <returns></returns>
    public static XmlElement YandexCreateMainElements(ref XmlDocument feed, ref XmlElement? root, ref XmlElement shop)
    {
        // Creating name, company, url of company, currencies and categories elements.
        XmlElement mainName = feed.CreateElement("name");
        XmlText mainNameText = feed.CreateTextNode("Яндекс Музей");
        mainName.AppendChild(mainNameText);

        XmlElement mainCompany = feed.CreateElement("company");
        XmlText mainCompanyText = feed.CreateTextNode("Яндекс");
        mainCompany.AppendChild(mainCompanyText);
        
        XmlElement mainUrl = feed.CreateElement("url");
        XmlText mainUrlText = feed.CreateTextNode("https://museum.yandex.ru/");
        mainUrl.AppendChild(mainUrlText);

        XmlElement currencies = feed.CreateElement("currencies");
        XmlElement currency = feed.CreateElement("currency");
        currency.SetAttribute("id", "RUR");
        currency.SetAttribute("rate", "1");

        currencies.AppendChild(currency);
        
        XmlElement categories = feed.CreateElement("categories");
        XmlElement mainCategory = feed.CreateElement("category");
        XmlText mainCategoryText = feed.CreateTextNode("Все товары");
        mainCategory.AppendChild(mainCategoryText);
        mainCategory.SetAttribute("id", "1");
        categories.AppendChild(mainCategory);
        
        // Adding elements in feed.
        shop.AppendChild(mainName);
        shop.AppendChild(mainCompany);
        shop.AppendChild(mainUrl);
        shop.AppendChild(currencies);
        shop.AppendChild(categories);

        return categories;
    }
    
    /// <summary>
    /// Creating element and adding it to the Google feed.
    /// </summary>
    /// <param name="itemElement">item element in our .xml file.</param>
    /// <param name="jsonItem">item from json file. </param>
    /// <param name="elementText">Dictionary of text elements. Key - text for our feed element, value - property name from .json.</param>
    /// <param name="feed">feed.json file data.</param>
    /// <param name="key">key from elementText dictionary. Required to make element with proper text.</param>
    /// <param name="namespaceUri">required parameter in creating element with essential "g:" prefix.</param>
    public static void GoogleCreateTypeElement(ref XmlElement itemElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key, string namespaceUri)
    {
        // Creating element with essential "g:" prefix in element's text.
        XmlElement typeElem = feed.CreateElement("g", key, namespaceUri);
        string? typeTextJson = jsonItem.GetProperty(elementText[key]).ToString();
        XmlText typeText = feed.CreateTextNode(typeTextJson);
        // Checking availability of item, then creating a proper text for element.
        if (typeTextJson == "True")
        {
            typeText = feed.CreateTextNode("in_stock");
        }
        else if (typeTextJson == "False")
        {
            typeText = feed.CreateTextNode("out_of_stock");
        }
        
        typeElem.AppendChild(typeText);
        itemElement.AppendChild(typeElem);
    }
    /// <summary>
    /// Creating currency element and adding it to the Google feed.
    /// </summary>
    /// <param name="itemElement">item element in our .xml file.</param>
    /// <param name="jsonItem">item from json file. </param>
    /// <param name="elementText">Dictionary of text elements. Key - text for our feed element, value - property name from .json.</param>
    /// <param name="feed">feed.json file data.</param>
    /// <param name="key">key from elementText dictionary. Required to make element with proper text.</param>
    /// <param name="namespaceUri">required parameter in creating element with essential "g:" prefix.</param>
    /// <param name="currency">currency of item.</param>
    public static void GoogleCreateTypeElement(ref XmlElement itemElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key, string namespaceUri, string currency)
    {
        // Creating element with essential "g:" prefix in element's text.
        XmlElement priceElem = feed.CreateElement("g", key, namespaceUri);
        JsonElement priceJson = jsonItem.GetProperty(elementText[key]);
        string? priceTextJson = priceJson.GetProperty("basePrice").ToString() + ".00 " + currency;
        XmlText priceText = feed.CreateTextNode(priceTextJson);
        
        priceElem.AppendChild(priceText);
        itemElement.AppendChild(priceElem);
    }
    
    /// <summary>
    /// Creating element and adding it to the Yandex feed.
    /// </summary>
    /// <param name="offerElement">offer element in our .xml file.</param>
    /// <param name="jsonItem">item from json file. </param>
    /// <param name="elementText">Dictionary of text elements. Key - text for our feed element, value - property name from .json.</param>
    /// <param name="feed">feed.json file data.</param>
    /// <param name="key">key from elementText dictionary. Required to make element with proper text.</param>
    public static void YandexCreateTypeElement(ref XmlElement offerElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key)
    {
        XmlElement typeElem = feed.CreateElement(key);
        string? typeTextJson = jsonItem.GetProperty(elementText[key]).ToString();
        XmlText typeText = feed.CreateTextNode(typeTextJson);
        
        typeElem.AppendChild(typeText);
        offerElement.AppendChild(typeElem);
    }
    /// <summary>
    /// Creating price element and adding it to the Yandex feed.
    /// </summary>
    /// <param name="offerElement">offer element in our .xml file.</param>
    /// <param name="jsonItem">item from json file. </param>
    /// <param name="elementText">Dictionary of text elements. Key - text for our feed element, value - property name from .json.</param>
    /// <param name="feed">feed.json file data.</param>
    /// <param name="key">key from elementText dictionary. Required to make element with proper text.</param>
    public static void YandexCreatePriceElement(ref XmlElement offerElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key)
    {
        XmlElement priceElem = feed.CreateElement(key);
        JsonElement priceJson = jsonItem.GetProperty(elementText[key]);
        // Option that helps to decide which property are we looking for: "basePrice" or "currencyId".
        string option = (key == "price") ? "basePrice" : key;
        string? priceTextJson = priceJson.GetProperty(option).ToString();
        XmlText priceText = feed.CreateTextNode(priceTextJson);
        
        priceElem.AppendChild(priceText);
        offerElement.AppendChild(priceElem);
    }
}