from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import json
import asyncio

# Constants
AVAILABLE_CITIES = ["Jeddah", "Dammam", "Al Khobar"]
RESTAURANT_TYPES = ["pizza", "burger", "seafood", "traditional", "cafe", "fine dining"]

# Mock API functions
def fetch_country_city_data(city: str) -> Dict:
    city_data = {
        "Jeddah": {"population": 4000000, "avg_income": "High", "tourist_flow": "Very High"},
        "Dammam": {"population": 1200000, "avg_income": "High", "tourist_flow": "Medium"},
        "Al Khobar": {"population": 600000, "avg_income": "Very High", "tourist_flow": "High"}
    }
    return city_data.get(city, {})

def fetch_nearby_categories(city: str, business_type: str) -> Dict:
    return {
        "similar_businesses": 12,
        "average_rating": 4.2,
        "market_saturation": "Medium"
    }

def fetch_dataset(city: str, business_type: str) -> Dict:
    return {
        "high_traffic_areas": ["Downtown", "Waterfront", "Mall Area"],
        "recommended_districts": ["District A", "District B"],
        "average_rent": "$5000-$8000/month"
    }

@dataclass
class ValidationResult:
    is_valid: bool
    confidence_score: float
    matched_value: Optional[str] = None
    reason: Optional[str] = None

class SimpleRAG:
    def __init__(self):
        self.knowledge_base = {
            "cities": {
                "Jeddah": ["jeddah", "jeddah city", "جدة"],
                "Dammam": ["dammam", "dammam city", "الدمام"],
                "Al Khobar": ["khobar", "al khobar", "الخبر"]
            },
            "restaurant_types": {
                "pizza": ["pizza", "pizzeria", "pizza restaurant"],
                "burger": ["burger", "hamburger", "burger joint"],
                "seafood": ["seafood", "fish restaurant", "maritime cuisine"],
                "traditional": ["traditional", "local food", "saudi cuisine"],
                "cafe": ["cafe", "coffee shop", "bistro"],
                "fine dining": ["fine dining", "upscale", "luxury restaurant"]
            },
            "city_data": {
                "Jeddah": {
                    "population": 4000000,
                    "avg_income": "High",
                    "tourist_flow": "Very High",
                    "business_climate": "Excellent",
                    "market_saturation": "Medium",
                    "growth_rate": "8.5%",
                    "popular_areas": ["Red Sea Mall Area", "Al Andalus", "Al Rawdah", "Al Khalidiyah"],
                    "key_metrics": {
                        "restaurant_density": "Medium",
                        "average_rent": "$8000-12000/month",
                        "workforce_availability": "High",
                        "competition_level": "High"
                    }
                },
                "Dammam": {
                    "population": 1200000,
                    "avg_income": "High",
                    "tourist_flow": "Medium",
                    "business_climate": "Good",
                    "market_saturation": "Low",
                    "growth_rate": "6.5%",
                    "popular_areas": ["Al Faisaliyah", "Al Qusur", "Al Mazruiyah"],
                    "key_metrics": {
                        "restaurant_density": "Low",
                        "average_rent": "$6000-9000/month",
                        "workforce_availability": "Medium",
                        "competition_level": "Medium"
                    }
                },
                "Al Khobar": {
                    "population": 600000,
                    "avg_income": "Very High",
                    "tourist_flow": "High",
                    "business_climate": "Very Good",
                    "market_saturation": "Medium",
                    "growth_rate": "7.5%",
                    "popular_areas": ["Corniche", "Al Ulaya", "Al Rakah"],
                    "key_metrics": {
                        "restaurant_density": "Medium",
                        "average_rent": "$7000-10000/month",
                        "workforce_availability": "High",
                        "competition_level": "Medium"
                    }
                }
            },
            "restaurant_data": {
                "pizza": {
                    "initial_cost": "$150,000-300,000",
                    "profit_margin": "20-25%",
                    "break_even": "18-24 months",
                    "market_demand": "High",
                    "competition": "High",
                    "target_audience": "Families, Young Adults",
                    "success_rate": "75%"
                },
                "burger": {
                    "initial_cost": "$100,000-250,000",
                    "profit_margin": "25-30%",
                    "break_even": "12-18 months",
                    "market_demand": "Very High",
                    "competition": "Very High",
                    "target_audience": "All Demographics",
                    "success_rate": "80%"
                },
                "seafood": {
                    "initial_cost": "$200,000-400,000",
                    "profit_margin": "30-35%",
                    "break_even": "24-30 months",
                    "market_demand": "Medium",
                    "competition": "Medium",
                    "target_audience": "Upper Middle Class, Tourists",
                    "success_rate": "65%"
                },
                "traditional": {
                    "initial_cost": "$150,000-350,000",
                    "profit_margin": "25-30%",
                    "break_even": "18-24 months",
                    "market_demand": "High",
                    "competition": "High",
                    "target_audience": "Locals, Tourists",
                    "success_rate": "70%"
                },
                "cafe": {
                    "initial_cost": "$80,000-200,000",
                    "profit_margin": "35-40%",
                    "break_even": "12-18 months",
                    "market_demand": "Very High",
                    "competition": "High",
                    "target_audience": "Young Professionals, Students",
                    "success_rate": "85%"
                },
                "fine dining": {
                    "initial_cost": "$300,000-600,000",
                    "profit_margin": "40-45%",
                    "break_even": "24-36 months",
                    "market_demand": "Medium",
                    "competition": "Low",
                    "target_audience": "High Income, Business Professionals",
                    "success_rate": "60%"
                }
            }
        }

    def validate_with_confidence(self, field_type: str, value: str) -> ValidationResult:
        if field_type not in self.knowledge_base:
            return ValidationResult(False, 0.0, reason="Unknown field type")
        
        value = value.lower()
        best_match = None
        highest_confidence = 0.0
        
        for canonical, variants in self.knowledge_base[field_type].items():
            for variant in variants:
                if value in variant.lower() or variant.lower() in value:
                    confidence = len(set(value.split()) & set(variant.lower().split())) / \
                               max(len(value.split()), len(variant.lower().split()))
                    if confidence > highest_confidence:
                        highest_confidence = confidence
                        best_match = canonical
                        
        if best_match and highest_confidence > 0.5:
            return ValidationResult(True, highest_confidence, best_match)
        return ValidationResult(False, highest_confidence)

    def get_city_data(self, city: str) -> Dict:
        """Get detailed data for a specific city"""
        return self.knowledge_base["city_data"].get(city, {})

    def get_restaurant_data(self, restaurant_type: str) -> Dict:
        """Get detailed data for a specific restaurant type"""
        return self.knowledge_base["restaurant_data"].get(restaurant_type, {})

    def get_relevant_data(self, context: Dict) -> Dict:
        """Return relevant data based on conversation context"""
        relevant_data = {
            "cities_overview": {
                city: {
                    "population": data["population"],
                    "tourist_flow": data["tourist_flow"],
                    "market_saturation": data["market_saturation"]
                } for city, data in self.knowledge_base["city_data"].items()
            },
            "restaurant_types_overview": {
                r_type: {
                    "profit_margin": data["profit_margin"],
                    "market_demand": data["market_demand"],
                    "success_rate": data["success_rate"]
                } for r_type, data in self.knowledge_base["restaurant_data"].items()
            }
        }

        # Add specific data if available in context
        if "city" in context.get("collected_info", {}):
            city = context["collected_info"]["city"]
            relevant_data["selected_city_data"] = self.get_city_data(city)

        if "restaurant_type" in context.get("collected_info", {}):
            r_type = context["collected_info"]["restaurant_type"]
            relevant_data["selected_restaurant_data"] = self.get_restaurant_data(r_type)

        return relevant_data

class ConversationState:
    def __init__(self):
        self.rag = SimpleRAG()
        self.last_action = None  # Add this line
        self.required_info = {
            "city": {
                "value": None,
                "required": True,
                "validation": lambda x: self.rag.validate_with_confidence("cities", x),
                "priority": 1,
                "asked": False,
                "options": ["Jeddah", "Dammam", "Al Khobar"]
            },
            "restaurant_type": {
                "value": None,
                "required": True,
                "validation": lambda x: self.rag.validate_with_confidence("restaurant_types", x),
                "priority": 2,
                "asked": False,
                "options": ["pizza", "burger", "seafood", "traditional", "cafe", "fine dining"]
            },
            "budget_range": {
                "value": None,
                "required": False,
                "validation": lambda x: ValidationResult(bool(x), 1.0) if x else ValidationResult(False, 0.0),
                "priority": 3,
                "asked": False,
                "options": None
            },
            "target_audience": {
                "value": None,
                "required": False,
                "validation": lambda x: ValidationResult(bool(x), 1.0) if x else ValidationResult(False, 0.0),
                "priority": 4,
                "asked": False,
                "options": None
            },
            "preferred_areas": {
                "value": None,
                "required": False,
                "validation": lambda x: ValidationResult(bool(x), 1.0) if x else ValidationResult(False, 0.0),
                "priority": 5,
                "asked": False,
                "options": None
            }
        }
        self.conversation_history = []
        self.interaction_count = 0

    def add_user_input(self, user_input: str):
        """Add user input to conversation history"""
        self.conversation_history.append({"role": "user", "content": user_input})
        self.interaction_count += 1

    def add_assistant_response(self, response: str):
        """Add assistant response to conversation history"""
        self.conversation_history.append({"role": "assistant", "content": response})

    def get_last_input(self) -> str:
        """Get the last user input"""
        for message in reversed(self.conversation_history):
            if message["role"] == "user":
                return message["content"]
        return ""

    def get_conversation_history(self) -> List[Dict]:
        """Get the full conversation history"""
        return self.conversation_history

    def is_ready_for_analysis(self) -> bool:
        """Check if all required information is collected and validated"""
        for field, info in self.required_info.items():
            if info["required"] and (
                info["value"] is None or 
                not info["validation"](info["value"]).is_valid
            ):
                return False
        return True

    def reset(self):
        """Reset the conversation state"""
        for field in self.required_info:
            self.required_info[field]["value"] = None
            self.required_info[field]["asked"] = False
        self.conversation_history = []
        self.interaction_count = 0

    def process_budget_input(self, input_str: str):
        """Special handler for budget inputs"""
        if any(word in input_str.lower() for word in ["premium", "high", "luxury"]):
            return "premium"
        elif any(word in input_str.lower() for word in ["medium", "moderate", "standard"]):
            return "medium"
        elif any(word in input_str.lower() for word in ["low", "budget", "affordable"]):
            return "low"
        elif "don't have" in input_str.lower() or "no budget" in input_str.lower():
            return "default premium"
        return None

    def get_fields_to_ask(self) -> List[str]:
            """Get fields that need to be asked about, including budget if not asked yet"""
            fields = []
            
            # Add required fields that are missing
            for field, info in self.required_info.items():
                if info["required"] and info["value"] is None:
                    fields.append(field)
            
            # Add budget if we haven't asked about it yet
            if (not self.required_info["budget_range"]["asked"] and 
                self.required_info["budget_range"]["value"] is None):
                fields.append("budget_range")
                
            return fields[:3]  # Return maximum 3 fields

    def mark_field_as_asked(self, field: str):
        """Mark a field as having been asked about"""
        if field in self.required_info:
            self.required_info[field]["asked"] = True

    def get_top_missing_fields(self, count: int = 3) -> List[str]:
        missing_fields = [
            (field, info) for field, info in self.required_info.items()
            if info["required"] and (
                info["value"] is None or 
                not info["validation"](info["value"]).is_valid
            )
        ]
        
        sorted_fields = sorted(missing_fields, key=lambda x: x[1]["priority"])
        return [field for field, _ in sorted_fields[:count]]



class PromptProcessor:
    def __init__(self, llm):
        self.llm = llm
        self.conversation_state = ConversationState()

        # Prompt for completion checking
        self.completion_check_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a business advisor analyzing user input for required information.
            Analyze the following input and extract any relevant information.
            
            Required information:
            - City (must be one of: {cities})
            - Restaurant type (must be one of: {types})
            
            Current input: {input}
            """),
            ("human", "Return your analysis in this exact JSON format, no other text: {{\"is_complete\": boolean, \"missing_required\": [\"field1\", \"field2\"], \"extracted_info\": {{\"city\": string or null, \"restaurant_type\": string or null, \"budget_range\": string or null, \"target_audience\": string or null, \"preferred_areas\": string or null}}}}")
        ])

        # Prompt for initial acknowledgment
        self.acknowledgment_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful business consultant. Generate a brief, friendly acknowledgment 
            of the user's input. Keep it to one sentence.
            
            User input: {input}"""),
            ("human", "Generate a brief acknowledgment.")
        ])

        # Prompt for generating follow-up questions
        self.question_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful business consultant. Generate follow-up questions based on the current context.
            
            Current context: {context}
            Missing required information: {required_fields}
            Optional information not yet discussed: {optional_fields}
            Available options: {available_options}
            
            Generate exactly three natural, conversational questions to gather the missing information.
            Prioritize required fields first."""),
            ("human", "Generate three appropriate follow-up questions.")
        ])

        # Add new prompt for optional fields response
        self.optional_response_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are analyzing a user's response to whether they want to specify optional details or use default values.
            
            Context: The user was asked if they want to specify optional details for: {optional_fields}
            User response: {input}
            
            Determine if the user wants to:
            1. Use default values
            2. Provide more details
            3. Unclear/needs clarification
            
            Return your analysis in this JSON format: {{"decision": "default"|"provide"|"unclear", "reason": "brief explanation"}}"""),
            ("human", "Analyze the user's intention.")
        ])
        
        self.optional_response_chain = (
            self.optional_response_prompt 
            | self.llm 
            | JsonOutputParser()
        )


        # Create the chains
        self.completion_chain = (
            self.completion_check_prompt 
            | self.llm 
            | JsonOutputParser()
        )
        
        self.acknowledgment_chain = (
            self.acknowledgment_prompt 
            | self.llm 
            | StrOutputParser()
        )
        
        self.question_chain = (
            self.question_prompt 
            | self.llm 
            | StrOutputParser()
        )

    async def process_input(self, user_input: str) -> Dict:
        # Update conversation state with new input
        self.conversation_state.add_user_input(user_input)
        
        # Check if user is responding to optional fields question
        if self.conversation_state.last_action == "ask_optional":
            optional_fields = [
                field for field, info in self.conversation_state.required_info.items()
                if not info["required"] and not info["asked"]
            ]
            
            # Use LLM to analyze response
            response_analysis = await self.optional_response_chain.ainvoke({
                "input": user_input,
                "optional_fields": ", ".join(optional_fields)
            })
            
            if response_analysis["decision"] == "default":
                # Set defaults for all remaining optional fields
                for field, info in self.conversation_state.required_info.items():
                    if not info["required"] and info["value"] is None:
                        self.conversation_state.required_info[field]["value"] = f"default_{field}"
                        self.conversation_state.required_info[field]["asked"] = True
                
                return {
                    "action": "analyze",
                    "collected_info": {
                        field: info["value"] 
                        for field, info in self.conversation_state.required_info.items()
                        if info["value"] is not None
                    }
                }
            elif response_analysis["decision"] == "provide":
                # User wants to provide more details
                return {
                    "action": "ask_followup",
                    "missing_fields": optional_fields[:3]  # Ask for up to 3 optional fields
                }
            else:
                # Unclear response, ask again
                return {
                    "action": "ask_optional",
                    "message": "I'm not sure if you want to specify optional details or use default values. Could you please clarify if you'd like to provide information about: "
                                f"{', '.join(optional_fields)}? Or should I use default values?"
                }

        # Process input to extract information
        completion_status = await self.completion_chain.ainvoke({
            "input": user_input,
            "cities": list(self.conversation_state.required_info["city"]["options"]),
            "types": list(self.conversation_state.required_info["restaurant_type"]["options"])
        })
        
        # Update state with extracted information
        for field, value in completion_status["extracted_info"].items():
            if value:
                validation_result = self.conversation_state.required_info[field]["validation"](value)
                if validation_result.is_valid:
                    self.conversation_state.required_info[field]["value"] = \
                        validation_result.matched_value or value
                    self.conversation_state.required_info[field]["asked"] = True

        # Get missing required fields
        required_fields = [
            field for field, info in self.conversation_state.required_info.items()
            if info["required"] and info["value"] is None
        ]

        # If all required fields are filled, but optional fields haven't been addressed
        if not required_fields:
            optional_fields = [
                field for field, info in self.conversation_state.required_info.items()
                if not info["required"] and not info["asked"]
            ]
            
            if optional_fields:
                self.conversation_state.last_action = "ask_optional"
                return {
                    "action": "ask_optional",
                    "message": "Would you like to specify any optional details, or should I use default values for: "
                                f"{', '.join(optional_fields)}?"
                }
            
            # Set defaults for any unset optional fields
            for field, info in self.conversation_state.required_info.items():
                if not info["required"] and info["value"] is None:
                    self.conversation_state.required_info[field]["value"] = f"default_{field}"

            self.conversation_state.last_action = "analyze"
            return {
                "action": "analyze",
                "collected_info": {
                    field: info["value"] 
                    for field, info in self.conversation_state.required_info.items()
                    if info["value"] is not None
                }
            }
        else:
            self.conversation_state.last_action = "ask_followup"
            return {
                "action": "ask_followup",
                "missing_fields": required_fields
            }
    
    def get_missing_fields(self) -> Tuple[List[str], List[str]]:
        """Separate missing fields into required and optional"""
        required = []
        optional = []
        
        for field, info in self.conversation_state.required_info.items():
            if info["value"] is None:
                if info["required"]:
                    required.append(field)
                elif not info["asked"]:
                    optional.append(field)
                    
        return required, optional

    async def generate_follow_up_questions(self, missing_fields: List[str]) -> str:
        # Generate acknowledgment
        acknowledgment = await self.acknowledgment_chain.ainvoke({
            "input": self.conversation_state.get_last_input()
        })

        # Get context for questions
        context = {
            "collected_info": {
                field: info["value"]
                for field, info in self.conversation_state.required_info.items()
                if info["value"] is not None
            },
            "missing_fields": missing_fields
        }

        # Get relevant data from RAG
        rag_data = self.conversation_state.rag.get_relevant_data(context)

        # Generate questions
        questions = await self.question_chain.ainvoke({
            "context": context,
            "required_fields": [f for f in missing_fields if self.conversation_state.required_info[f]["required"]],
            "optional_fields": [f for f in missing_fields if not self.conversation_state.required_info[f]["required"]],
            "available_options": {
                "cities": list(self.conversation_state.required_info["city"]["options"]),
                "types": list(self.conversation_state.required_info["restaurant_type"]["options"])
            }
        })

        return f"{acknowledgment}\n\n{questions}"

class RestaurantConsultant:
    def __init__(self, openai_api_key: str):
        self.llm = ChatOpenAI(
            temperature=0.7,
            model="gpt-4",
            api_key=openai_api_key
        )
        self.prompt_processor = PromptProcessor(self.llm)
        
        self.system_message = SystemMessage(content=f"""
        You are a specialized restaurant location consultant with expertise in {', '.join(AVAILABLE_CITIES)}. 
        You help entrepreneurs find the best locations for their restaurants in these cities only.
        
        Your role is to:
        1. Process information about restaurant location requirements
        2. Consider cultural and market factors in your analysis
        3. Provide specific, actionable recommendations
        4. Maintain context throughout the conversation
        5. Be clear and precise in your advice
        
        Available cities: {', '.join(AVAILABLE_CITIES)}
        Available restaurant types: {', '.join(RESTAURANT_TYPES)}
        """)
        
        self.messages = [self.system_message]
        
        self.prompt = ChatPromptTemplate.from_messages([
            self.system_message,
            MessagesPlaceholder(variable_name="history"),
            ("human", "{input}")
        ])
        
        self.chain = self.prompt | self.llm | StrOutputParser()

    async def generate_analysis(self, collected_info: Dict) -> str:
        analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", """Based on the following information and analysis, provide a comprehensive 
            location recommendation for a restaurant business. Consider market factors, competition,
            and local preferences in your analysis.
            
            Use the data provided to make specific, actionable recommendations."""),
            ("human", """
            Collected Information:
            {collected_info}
            
            Analysis Data:
            {analysis_data}
            
            Please provide your recommendation.
            """)
        ])
        
        city = collected_info.get("city")
        business_type = collected_info.get("restaurant_type")
        analysis = {
            "city_data": fetch_country_city_data(city),
            "competition": fetch_nearby_categories(city, business_type),
            "locations": fetch_dataset(city, business_type)
        }
        
        response = analysis_prompt | self.llm | StrOutputParser()
        return response.invoke({
            "collected_info": json.dumps(collected_info, indent=2),
            "analysis_data": json.dumps(analysis, indent=2)
        })

    async def chat(self, user_input: str) -> str:
        result = await self.prompt_processor.process_input(user_input)
        
        if result["action"] == "analyze":
            return await self.generate_analysis(result["collected_info"])
        elif result["action"] == "ask_optional":
            return result["message"]
        else:
            return await self.prompt_processor.generate_follow_up_questions(
                result["missing_fields"]
            )

async def main():
    with open('secrets/secret_LLM_api_key.json') as f:
        api_key = json.load(f)['api_key']
    
    consultant = RestaurantConsultant(openai_api_key=api_key)

    
    print("Restaurant Location Consultant AI (Type 'quit' to exit)")
    print(f"Available cities: {', '.join(AVAILABLE_CITIES)}")
    
    while True:
        user_input = input("\nYou: ")
        if user_input.lower() == 'quit':
            break
            
        response = await consultant.chat(user_input)
        print(f"\nConsultant: {response}")

if __name__ == "__main__":
    asyncio.run(main())
