import asyncio
import re
import pandas as pd
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChatbotClassifier:
    def __init__(self):
        self.timeout = 45000
        self.concurrency = 30
        
        # Advanced chatbot classification patterns
        self.chatbot_types = {
            'ai_powered': {
                'patterns': [
                    'ai chat', 'artificial intelligence', 'machine learning', 'natural language',
                    'ai assistant', 'smart chat', 'intelligent chat', 'automated responses',
                    'ai powered', 'chatgpt', 'openai', 'ai bot', 'virtual assistant'
                ],
                'services': ['dialogflow', 'watson', 'luis', 'wit.ai', 'rasa', 'botframework'],
                'priority': 'HIGH_COMPETITION'
            },
            'live_agent_only': {
                'patterns': [
                    'live agent', 'human support', 'talk to agent', 'connect with agent',
                    'real person', 'human chat', 'live support', 'agent available',
                    'speak with someone', 'customer service team'
                ],
                'services': ['zendesk', 'freshchat', 'livechat', 'olark'],
                'priority': 'MEDIUM_COMPETITION'
            },
            'contact_form_disguised': {
                'patterns': [
                    'leave message', 'send email', 'contact form', 'get in touch',
                    'we\'ll get back', 'will respond', 'email us', 'submit inquiry',
                    'fill out form', 'send us message'
                ],
                'services': [],
                'priority': 'LOW_COMPETITION'
            },
            'social_messenger_only': {
                'patterns': [
                    'facebook messenger', 'whatsapp chat', 'telegram', 'instagram chat',
                    'social media', 'message us on'
                ],
                'services': ['messenger', 'whatsapp', 'telegram'],
                'priority': 'LOW_COMPETITION'
            },
            'help_desk_tickets': {
                'patterns': [
                    'create ticket', 'support ticket', 'help desk', 'submit ticket',
                    'issue tracker', 'bug report', 'technical support'
                ],
                'services': ['zendesk', 'freshdesk', 'jira', 'servicenow'],
                'priority': 'NO_COMPETITION'
            },
            'basic_popup': {
                'patterns': [
                    'need help?', 'questions?', 'how can we help', 'contact us',
                    'get support', 'ask question'
                ],
                'services': [],
                'priority': 'LOW_COMPETITION'
            }
        }
        
        # Gamified/Interactive features (what you offer)
        self.gamification_features = [
            'quiz', 'game', 'interactive', 'choose your', 'personalized',
            'recommendation engine', 'guided selection', 'product finder',
            'adventure finder', 'trip planner', 'experience matcher'
        ]

    async def classify_chatbot_type(self, page, url):
        """Classify the type of chatbot/chat solution."""
        try:
            await page.goto(url, timeout=self.timeout, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)
            
            # Get page content
            html_content = await page.content()
            text_content = await page.evaluate('document.body.innerText || ""')
            combined_content = (html_content + " " + text_content).lower()
            
            classification = {
                'chatbot_type': 'unknown',
                'priority_level': 'UNKNOWN',
                'features_found': [],
                'competitive_threat': 'UNKNOWN',
                'still_prospect': False,
                'reasoning': []
            }
            
            # Check for gamification (your competitive advantage)
            has_gamification = any(feature in combined_content for feature in self.gamification_features)
            
            # Classify chatbot type
            detected_types = []
            
            for chatbot_type, config in self.chatbot_types.items():
                # Check patterns
                pattern_matches = sum(1 for pattern in config['patterns'] if pattern in combined_content)
                
                # Check services
                service_matches = sum(1 for service in config['services'] if service in combined_content)
                
                # Calculate confidence
                total_indicators = len(config['patterns']) + len(config['services'])
                if total_indicators > 0:
                    confidence = (pattern_matches + service_matches * 2) / total_indicators
                    
                    if confidence > 0.1:  # 10% threshold
                        detected_types.append({
                            'type': chatbot_type,
                            'confidence': confidence,
                            'priority': config['priority'],
                            'pattern_matches': pattern_matches,
                            'service_matches': service_matches
                        })
            
            # Determine primary type
            if detected_types:
                # Sort by confidence
                detected_types.sort(key=lambda x: x['confidence'], reverse=True)
                primary_type = detected_types[0]
                
                classification['chatbot_type'] = primary_type['type']
                classification['priority_level'] = primary_type['priority']
                classification['features_found'] = [t['type'] for t in detected_types[:3]]
                
                # Determine if still a prospect
                if primary_type['priority'] in ['LOW_COMPETITION', 'NO_COMPETITION']:
                    classification['still_prospect'] = True
                    classification['competitive_threat'] = 'LOW'
                    classification['reasoning'].append(f"Has {primary_type['type']} - not competitive with AI gamified chatbot")
                
                elif primary_type['priority'] == 'MEDIUM_COMPETITION' and not has_gamification:
                    classification['still_prospect'] = True
                    classification['competitive_threat'] = 'MEDIUM'
                    classification['reasoning'].append("Has live agent chat but no gamification - still opportunity")
                
                elif primary_type['priority'] == 'HIGH_COMPETITION':
                    classification['still_prospect'] = False
                    classification['competitive_threat'] = 'HIGH'
                    classification['reasoning'].append("Has advanced AI chatbot - strong competition")
                
                else:
                    classification['competitive_threat'] = 'MEDIUM'
            
            # Special case: Check for advanced features
            advanced_features = [
                'natural language processing', 'machine learning', 'ai powered',
                'intelligent responses', 'contextual chat', 'conversational ai'
            ]
            
            if any(feature in combined_content for feature in advanced_features):
                classification['still_prospect'] = False
                classification['competitive_threat'] = 'HIGH'
                classification['reasoning'].append("Has advanced AI features")
            
            # Final gamification check
            if has_gamification:
                classification['still_prospect'] = False
                classification['competitive_threat'] = 'HIGH'
                classification['reasoning'].append("Already has gamified experience")
            
            return classification
            
        except Exception as e:
            logger.warning(f"Error classifying {url}: {e}")
            return {
                'chatbot_type': 'error',
                'priority_level': 'ERROR',
                'features_found': [],
                'competitive_threat': 'UNKNOWN',
                'still_prospect': False,
                'reasoning': [f"Analysis failed: {str(e)[:100]}"]
            }

    async def process_chatbot_companies(self, input_csv, output_csv, batch_size=50):
        """Process companies that were flagged as having chatbots."""
        try:
            df = pd.read_csv(input_csv)
            
            print(f"\n🔍 CSV ANALYSIS")
            print(f"📊 Total companies in CSV: {len(df)}")
            print(f"📋 Has chatbot column values: {df['has_chatbot'].value_counts().to_dict()}")
            
            # More flexible filtering for companies with chatbots
            chatbot_companies = df[
                (df['has_chatbot'] == 'True') | 
                (df['has_chatbot'] == True) | 
                (df['has_chatbot'] == 'Yes') |
                (df['has_chatbot'] == 'true') |
                (df['has_chatbot'] == 1)
            ].copy()
            
            print(f"\n🔍 CHATBOT CLASSIFICATION ANALYSIS")
            print(f"📊 Total companies with 'chatbots': {len(chatbot_companies)}")
            
            if len(chatbot_companies) == 0:
                print("❌ No companies with chatbots found!")
                print("💡 Check your has_chatbot column values above")
                return
            
            print(f"🎯 Starting detailed classification...")
            
            # Add new classification columns
            new_columns = [
                'chatbot_type', 'priority_level', 'features_found', 
                'competitive_threat', 'still_prospect', 'classification_reasoning',
                'reclassified_date'
            ]
            
            for col in new_columns:
                if col not in chatbot_companies.columns:
                    chatbot_companies[col] = pd.NA
            
            # Find URL column
            url_columns = ['Website URL', 'website url', 'url', 'domain', 'website', 'company_url', 'site', 'Website', 'URL', 'Domain']
            url_column = None
            for col in url_columns:
                if col in chatbot_companies.columns:
                    url_column = col
                    break
            
            if not url_column:
                print("❌ No URL column found!")
                return
            
            print(f"✅ Found URL column: {url_column}")
            
            # Process in batches
            semaphore = asyncio.Semaphore(self.concurrency)
            
            async def classify_with_semaphore(browser, index, company_name, url):
                async with semaphore:
                    try:
                        classification = await self.classify_chatbot_type_with_browser(browser, url)
                        
                        # Update dataframe
                        chatbot_companies.loc[index, 'chatbot_type'] = classification['chatbot_type']
                        chatbot_companies.loc[index, 'priority_level'] = classification['priority_level']
                        chatbot_companies.loc[index, 'features_found'] = '; '.join(classification['features_found'])
                        chatbot_companies.loc[index, 'competitive_threat'] = classification['competitive_threat']
                        chatbot_companies.loc[index, 'still_prospect'] = 'True' if classification['still_prospect'] else 'False'
                        chatbot_companies.loc[index, 'classification_reasoning'] = '; '.join(classification['reasoning'])
                        chatbot_companies.loc[index, 'reclassified_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        prospect_status = "✅ STILL PROSPECT" if classification['still_prospect'] else "❌ NOT PROSPECT"
                        logger.info(f"📋 {company_name}: {classification['chatbot_type']} | {prospect_status}")
                        
                    except Exception as e:
                        chatbot_companies.loc[index, 'chatbot_type'] = 'error'
                        chatbot_companies.loc[index, 'classification_reasoning'] = f"Error: {str(e)[:100]}"
                        logger.error(f"❌ {company_name}: Classification failed")
            
            # Process batches
            unprocessed = chatbot_companies[chatbot_companies['chatbot_type'].isna()].index.tolist()
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                
                while unprocessed:
                    batch_indices = unprocessed[:batch_size]
                    tasks = []
                    
                    for index in batch_indices:
                        row = chatbot_companies.loc[index]
                        clean_url = self.clean_url(row.get(url_column))
                        if clean_url:
                            company_name = row.get('Company Name', f'Company {index}')
                            tasks.append(classify_with_semaphore(browser, index, company_name, clean_url))
                    
                    if tasks:
                        print(f"\n🔄 Processing batch: {len(tasks)} companies")
                        await asyncio.gather(*tasks)
                    
                    # Save progress
                    chatbot_companies.to_csv(output_csv, index=False)
                    
                    # Update unprocessed list
                    unprocessed = chatbot_companies[chatbot_companies['chatbot_type'].isna()].index.tolist()
                    
                    if unprocessed:
                        await asyncio.sleep(5)  # Rest between batches
                
                await browser.close()
            
            # Generate final report
            self.generate_classification_report(chatbot_companies, output_csv)
            
        except Exception as e:
            print(f"❌ Error during classification: {e}")
            import traceback
            traceback.print_exc()

    async def classify_chatbot_type_with_browser(self, browser, url):
        """Classify chatbot type using browser instance."""
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            result = await self.classify_chatbot_type(page, url)
            return result
        finally:
            await context.close()

    def clean_url(self, url):
        """Clean and validate URL."""
        if not url or pd.isna(url):
            return None
        url = str(url).strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

    def generate_classification_report(self, df, output_csv):
        """Generate detailed classification report."""
        print(f"\n" + "="*80)
        print("🎯 CHATBOT CLASSIFICATION REPORT")
        print("="*80)
        
        # Count by chatbot type
        type_counts = df['chatbot_type'].value_counts()
        print(f"\n📊 CHATBOT TYPES FOUND:")
        for chatbot_type, count in type_counts.items():
            print(f"   {chatbot_type}: {count} companies")
        
        # Count by competitive threat
        threat_counts = df['competitive_threat'].value_counts()
        print(f"\n⚔️ COMPETITIVE THREAT LEVELS:")
        for threat, count in threat_counts.items():
            print(f"   {threat}: {count} companies")
        
        # Most important: Still prospects
        still_prospects = len(df[df['still_prospect'] == 'True'])
        not_prospects = len(df[df['still_prospect'] == 'False'])
        
        print(f"\n🎯 RECLASSIFICATION RESULTS:")
        print(f"✅ STILL PROSPECTS: {still_prospects} companies")
        print(f"❌ CONFIRMED NON-PROSPECTS: {not_prospects} companies")
        print(f"📈 Recovery rate: {still_prospects/(still_prospects+not_prospects)*100:.1f}%")
        
        # Breakdown of recoverable prospects
        recoverable_types = df[df['still_prospect'] == 'True']['chatbot_type'].value_counts()
        print(f"\n🔄 RECOVERABLE PROSPECT TYPES:")
        for prospect_type, count in recoverable_types.items():
            print(f"   {prospect_type}: {count} companies")
        
        print(f"\n📁 DETAILED RESULTS SAVED TO: {output_csv}")
        print("="*80)

def main():
    """Main function for chatbot classification."""
    print("🔍 CHATBOT TYPE CLASSIFIER")
    print("=" * 50)
    
    input_csv = input("Enter your analysis results CSV (default: FULL_analysis_results.csv): ").strip() or 'FULL_analysis_results.csv'
    output_csv = input("Enter output filename (default: chatbot_classification_results.csv): ").strip() or 'chatbot_classification_results.csv'
    batch_size = int(input("Batch size (default: 50): ").strip() or '50')
    
    classifier = ChatbotClassifier()
    asyncio.run(classifier.process_chatbot_companies(input_csv, output_csv, batch_size))
    
    print(f"\n🎉 Classification complete!")
    print(f"💡 Check '{output_csv}' for detailed chatbot analysis")

if __name__ == "__main__":
    main()