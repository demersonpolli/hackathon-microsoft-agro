
using Azure;
using Azure.AI.OpenAI;
using hackaton_microsoft_agro.Data;
using OpenAI.Chat;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;


namespace hackaton_microsoft_agro.Services
{

    public class OpenAIService(string endpoint, string apiKey, string deploymentName)
    {
        AzureOpenAIClient client = new AzureOpenAIClient(new Uri(endpoint), new AzureKeyCredential(apiKey));

        public string ProcessResponse(string query, string sources)
        {
            string GROUNDED_PROMPT = $"""
                                    You are a friendly assistant who assists agricultural professionals with planting and pest control in soybeans.
                                    Answer the query using only the sources provided below in a friendly and concise bulleted manner.
                                    Answer ONLY with the facts listed in the list of sources below or user previous message.
                                    If there isn't enough information below, say you don't know. 
                                    Put source in the end. Do search in portugues and response in english.
                                    Query: {query}
                                    Sources:\n {sources}
                                    """;
            try
            {
                List<ChatMessage> chatMessages = new List<ChatMessage>();

                chatMessages.Add(ChatMessage.CreateUserMessage(GROUNDED_PROMPT));

                var response = client.GetChatClient(deploymentName).CompleteChat(chatMessages);
                return response.Value.Content[0].Text;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error ProcessResponse -> " + ex.Message);
            }
        }

        public string ProcessResponseForPlanCropField(string query, string city, string sources)
        {
            string GROUNDED_PROMPT = $"""
                                    You are a friendly assistant who assists agricultural professionals in planing crop field for soybeans.
                                    Answer the query using the sources provided below in a friendly and detailed.
                                    Do search in portugues and response in english.


                                    Template:
                                        Location: {city}
                                        Planting date: 11/12/2025
                                        Estimated emergence date:
                                        Estimated harvest date:
                                        Soyabem Cultivar:
                                        Planting system: no-till.
                                        Previous crop: millet + brachiaria
                                        Row spacing: 45 cm
                                        Maintenance fertilization:
                                        Seed treatment:
                                        Inoculant:
                                        Micronutrients:
                                        Insecticides:
                                        Fungicides:

                                    Query: {query}
                                    Sources:\n{sources}


                                    """;
            try
            {
                var response = client.GetChatClient(deploymentName).CompleteChat(
                    ChatMessage.CreateSystemMessage("Use system information for recomendations"),
                    ChatMessage.CreateSystemMessage(SoyInformation.TEMPLATE),
                    ChatMessage.CreateSystemMessage(SoyInformation.HERBICIDES),
                    ChatMessage.CreateSystemMessage(SoyInformation.FUNGICIDES),
                    ChatMessage.CreateSystemMessage(SoyInformation.INSETICIDES),
                    ChatMessage.CreateSystemMessage(SoyInformation.SEEDS),
                    ChatMessage.CreateSystemMessage(SoyInformation.SOIL_FERTILITY),
                    ChatMessage.CreateUserMessage(GROUNDED_PROMPT)
                    );
                return response.Value.Content[0].Text;
            }
            catch (Exception ex)
            {
                throw new Exception("Error ProcessResponse -> " + ex.Message);
            }
        }

    }
}