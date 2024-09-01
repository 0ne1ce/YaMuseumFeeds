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

    public void FunctionHandler(string i) {
        Main();
    }

    public static void Main()
    {
        client = new AmazonS3Client(configS3);
        WritingAnObjectAsync().Wait();
    }
    
    static async Task WritingAnObjectAsync()
    {
        try
        {
            int categoryAdditionalId = 100;
            Dictionary<string, string> categoriesDictionary = new Dictionary<string, string>();

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
                //Console.WriteLine("Object metadata, Title: {0}", title);
                //Console.WriteLine("Content type: {0}", contentType);

                requestedData = reader.ReadToEnd();
            }

            var putRequestGoogle = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = googleKeyName,
                ContentBody = GoogleFeedCreationProcess(requestedData)
            };

            PutObjectResponse responseGoogle = await client.PutObjectAsync(putRequestGoogle);

            var putRequestYandex = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = yandexKeyName,
                ContentBody = YandexFeedCreationProcess(requestedData, ref categoryAdditionalId,
                    categoriesDictionary)
            };

            PutObjectResponse responseYandex = await client.PutObjectAsync(putRequestYandex);
        }
        catch (AmazonS3Exception e)
        {
            Console.WriteLine(e.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }

    public static string GoogleFeedCreationProcess(string fileNamePath)
    {
        //string data = File.ReadAllText(fileNamePath);
        string data = fileNamePath;
        
        XmlDocument googleFeed = new XmlDocument();
        string namespaceUri = "http://base.google.com/ns/1.0";
        string version = "2.0";

        XmlElement rss = googleFeed.CreateElement("rss");
        rss.SetAttribute("xmlns:g", namespaceUri);
        rss.SetAttribute("version", version);
        
        googleFeed.AppendChild(rss);
            
        XmlElement channelElem = googleFeed.CreateElement("channel");
            
        GoogleCreateMainElements(ref googleFeed, ref rss, ref channelElem);

        using var jsonFile = JsonDocument.Parse(data);
        JsonElement jsonRoot = jsonFile.RootElement;
        
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
        
        for (int i = 0; i < jsonRoot.GetArrayLength(); i++)
        {
            XmlElement itemElem = googleFeed.CreateElement("item");
                
            JsonElement item = jsonRoot[i];
                
            string? idTextJson = item.GetProperty("offerId").ToString();
                
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "id", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "title", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "description", namespaceUri);
                
            XmlElement linkElem = googleFeed.CreateElement("g", "link", namespaceUri);
            XmlText linkText = googleFeed.CreateTextNode($@"https://museum.yandex.ru/product/{idTextJson}");
            linkElem.AppendChild(linkText);
            itemElem.AppendChild(linkElem);
            
            XmlElement imageLinkElem = googleFeed.CreateElement("g", "image_link", namespaceUri);
            JsonElement imageLinkArray = item.GetProperty("pictures");
            
            JsonElement firstPicJson = imageLinkArray[0];
            GoogleCreateTypeElement(ref itemElem, ref firstPicJson, ElementTextValues, ref googleFeed, "image_link", namespaceUri);
            
            for (int j = 1; j < imageLinkArray.GetArrayLength(); j++)
            {
                JsonElement picJson = imageLinkArray[j];
                
                GoogleCreateTypeElement(ref itemElem,ref picJson, ElementTextValues, ref googleFeed, "additional_image_link", namespaceUri);
            }

            XmlElement conditionElem = googleFeed.CreateElement("g", "condition", namespaceUri);
            XmlText conditionText = googleFeed.CreateTextNode("новый");

            conditionElem.AppendChild(conditionText);
            itemElem.AppendChild(conditionElem);
            
            JsonElement price = item.GetProperty("price");
            string? currency = (price.GetProperty("currencyId").ToString() == "RUR") ? "RUB" : price.GetProperty("currencyId").ToString();
            
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "availability", namespaceUri);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "price", namespaceUri, currency);
            GoogleCreateTypeElement(ref itemElem, ref item, ElementTextValues, ref googleFeed, "brand", namespaceUri);
            
            channelElem.AppendChild(itemElem);
                
            rss?.AppendChild(channelElem);
        }
        
        // bye bye <3
        //googleFeed.Save(newFilePath);
        Console.WriteLine("File saved successfully!");

        return googleFeed.OuterXml;
    }

    public static string YandexFeedCreationProcess(string fileNamePath, ref int categoryAdditionalId,
        Dictionary<string, string> categoriesDictionary)
    {
        //string data = File.ReadAllText(fileNamePath);
        string data = fileNamePath;
        
        XmlDocument yandexFeed = new XmlDocument();

        XmlElement yml_catalog = yandexFeed.CreateElement("yml_catalog");
        DateTime date1 = DateTime.Now;
        yml_catalog.SetAttribute("date", date1.ToString());
        yandexFeed.AppendChild(yml_catalog);
        
        XmlElement shop = yandexFeed.CreateElement("shop");
        
        XmlElement categories = YandexCreateMainElements(ref yandexFeed, ref yml_catalog, ref shop);

        XmlElement offers = yandexFeed.CreateElement("offers");
        
        using var jsonFile = JsonDocument.Parse(data);
        JsonElement jsonRoot = jsonFile.RootElement;
        
        Dictionary<string, string> ElementTextValues = new Dictionary<string, string>()
        {
            {"name", "title"},
            {"price", "price"},
            {"currencyId", "price"},
            {"vendor", "vendor"},
            {"picture", "url"},
            {"description", "description"}
        };
        
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
            
            YandexCreatePriceElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "price");
            YandexCreatePriceElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "currencyId");
            
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

            XmlElement categoryId = yandexFeed.CreateElement("categoryId");
            XmlText categoryIdText = yandexFeed.CreateTextNode(categoriesDictionary[otherCategory]);
            categoryId.AppendChild(categoryIdText);
            offerElem.AppendChild(categoryId);
            
            YandexCreateTypeElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "vendor");
            
            JsonElement imageLinkArray = item.GetProperty("pictures");
            for (int j = 0; j < imageLinkArray.GetArrayLength(); j++)
            {
                JsonElement picJson = imageLinkArray[j];

                YandexCreateTypeElement(ref offerElem, ref picJson, ElementTextValues, ref yandexFeed, "picture");
            }
            
            YandexCreateTypeElement(ref offerElem, ref item, ElementTextValues, ref yandexFeed, "description");
            
            offers.AppendChild(offerElem);
        }
        foreach (var element in categoriesDictionary)
        {
            XmlText categoryText = yandexFeed.CreateTextNode(element.Key);
            XmlElement category = yandexFeed.CreateElement("category");
            category.SetAttribute("id", element.Value);
            category.SetAttribute("parentId", "1");
            category.AppendChild(categoryText);
            categories.AppendChild(category);
        }
        shop.AppendChild(offers);

        yml_catalog.AppendChild(shop);
        // bye bye <3
        //yandexFeed.Save(newFilePath);
        Console.WriteLine("File saved successfully!");

        return yandexFeed.OuterXml;
    }
    
    public static void GoogleCreateMainElements(ref XmlDocument feed, ref XmlElement? root, ref XmlElement channel)
    {
        XmlElement mainTitleElem = feed.CreateElement("title");
        XmlText mainTitleText = feed.CreateTextNode("Яндекс Музей");
        mainTitleElem.AppendChild(mainTitleText);
            
        XmlElement mainLinkElem = feed.CreateElement("link");
        XmlText mainLinkText = feed.CreateTextNode("https://museum.yandex.ru");
        mainLinkElem.AppendChild(mainLinkText);
            
        XmlElement mainDescriptionElem = feed.CreateElement("description");
        XmlText mainDescriptionText = feed.CreateTextNode("Фид Яндекс Музея");
        mainDescriptionElem.AppendChild(mainDescriptionText);

        channel.AppendChild(mainTitleElem);
        channel.AppendChild(mainLinkElem);
        channel.AppendChild(mainDescriptionElem);

        root?.AppendChild(channel);
    }

    public static XmlElement YandexCreateMainElements(ref XmlDocument feed, ref XmlElement? root, ref XmlElement shop)
    {
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
        
        shop.AppendChild(mainName);
        shop.AppendChild(mainCompany);
        shop.AppendChild(mainUrl);
        shop.AppendChild(currencies);
        shop.AppendChild(categories);

        return categories;


    }

    public static void GoogleCreateTypeElement(ref XmlElement itemElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key, string namespaceUri)
    {
        XmlElement typeElem = feed.CreateElement("g", key, namespaceUri);
        string? typeTextJson = jsonItem.GetProperty(elementText[key]).ToString();
        XmlText typeText = feed.CreateTextNode(typeTextJson);
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
    
    public static void GoogleCreateTypeElement(ref XmlElement itemElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key, string namespaceUri, string currency)
    {
        XmlElement priceElem = feed.CreateElement("g", key, namespaceUri);
        JsonElement priceJson = jsonItem.GetProperty(elementText[key]);
        string? priceTextJson = priceJson.GetProperty("basePrice").ToString() + ".00 " + currency;
        XmlText priceText = feed.CreateTextNode(priceTextJson);
        
        priceElem.AppendChild(priceText);
        itemElement.AppendChild(priceElem);
    }
    
    public static void YandexCreateTypeElement(ref XmlElement offerElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key)
    {
        XmlElement typeElem = feed.CreateElement(key);
        string? typeTextJson = jsonItem.GetProperty(elementText[key]).ToString();
        XmlText typeText = feed.CreateTextNode(typeTextJson);
        
        typeElem.AppendChild(typeText);
        offerElement.AppendChild(typeElem);
    }
    
    public static void YandexCreatePriceElement(ref XmlElement itemElement, ref JsonElement jsonItem, Dictionary<string, string> elementText, 
        ref XmlDocument feed, string key)
    {
        XmlElement priceElem = feed.CreateElement(key);
        JsonElement priceJson = jsonItem.GetProperty(elementText[key]);
        string option = (key == "price") ? "basePrice" : key;
        string? priceTextJson = priceJson.GetProperty(option).ToString();
        XmlText priceText = feed.CreateTextNode(priceTextJson);
        
        priceElem.AppendChild(priceText);
        itemElement.AppendChild(priceElem);
    }
}