import asyncio
import re
import pandas as pd
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import logging
import json
import time
from datetime import datetime
import os
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ContinuousTourOperatorAnalyzer:
    def __init__(self, ram_gb=48):
        # Adaptive settings based on available RAM
        if ram_gb >= 32:
            self.aggressive_settings = {
                'concurrency': 45,
                'batch_size': 80,
                'timeout': 45000,
                'delay_between_batches': 30,
                'max_pages_per_site': 3
            }
            self.conservative_settings = {
                'concurrency': 20,
                'batch_size': 40,
                'timeout': 75000,
                'delay_between_batches': 60,
                'max_pages_per_site': 5
            }
        else:
            # Fallback for lower RAM
            self.aggressive_settings = {
                'concurrency': 20,
                'batch_size': 30,
                'timeout': 45000,
                'delay_between_batches': 45,
                'max_pages_per_site': 3
            }
            self.conservative_settings = {
                'concurrency': 10,
                'batch_size': 20,
                'timeout': 75000,
                'delay_between_batches': 90,
                'max_pages_per_site': 5
            }
        
        self.patient_settings = {
            'concurrency': 5,
            'batch_size': 10,
            'timeout': 120000,
            'delay_between_batches': 120,
            'max_pages_per_site': 7
        }
        
        # Current settings (starts with aggressive)
        self.current_settings = self.aggressive_settings.copy()
        
        # Detection patterns
        self.chatbot_patterns = {
            'intercom': ['intercom', 'widget.intercom.io'],
            'zendesk': ['zendesk', 'zopim', 'zdchat'],
            'drift': ['drift.com', 'js.driftt.com'],
            'tidio': ['tidio', 'code.tidio.co'],
            'tawk': ['tawk.to', 'embed.tawk.to'],
            'livechat': ['livechatinc', 'cdn.livechatinc.com'],
            'crisp': ['crisp.chat', 'client.crisp.chat'],
            'hubspot': ['hubspot', 'js.hs-analytics.net'],
            'freshchat': ['freshchat', 'wchat.freshchat.com'],
            'olark': ['olark.com', 'static.olark.com'],
            'smartsupp': ['smartsupp.com'],
            'chatlio': ['chatlio.com'],
            'pure_chat': ['purechat.com'],
            'chat_widget': ['chat-widget', 'chatwidget', 'chat_widget'],
            'messenger': ['messenger', 'connect.facebook.net']
        }
        
        self.booking_patterns = {
            'fareharbor': ['fareharbor.com', 'fareharbor', 'fh-button', 'fh-widget'],
            'resd': ['resd.com', 'res-d.com', 'resd-booking'],
            'woocommerce': ['woocommerce', 'wc-', 'wp-content/plugins/woocommerce'],
            'shopify': ['shopify', 'cdn.shopify.com', 'shop.app'],
            'bookeo': ['bookeo.com', 'bookeo'],
            'checkfront': ['checkfront.com', 'checkfront'],
            'peek': ['peek.com', 'bookwithpeek'],
            'rezdy': ['rezdy.com', 'rezdy'],
            'regiondo': ['regiondo.com', 'regiondo'],
            'trekksoft': ['trekksoft.com', 'trekksoft'],
            'bokun': ['bokun.io', 'bokun.com'],
            'viator': ['viator.com', 'viator'],
            'stripe': ['stripe.com', 'js.stripe.com'],
            'square': ['squareup.com', 'square'],
            'paypal': ['paypal.com', 'paypal'],
            'book_now': ['book-now', 'booknow', 'reservation'],
            'calendar_booking': ['calendly.com', 'acuityscheduling']
        }
        
        self.ota_patterns = {
            'getyourguide': ['getyourguide.com', 'getyourguide'],
            'viator': ['viator.com', 'tripadvisor'],
            'tripadvisor': ['tripadvisor.com', 'tripadvisor'],
            'expedia': ['expedia.com', 'expedia'],
            'airbnb': ['airbnb.com', 'airbnb'],
            'booking': ['booking.com', 'booking'],
            'klook': ['klook.com', 'klook'],
            'tiqets': ['tiqets.com', 'tiqets'],
            'headout': ['headout.com', 'headout'],
            'musement': ['musement.com', 'musement'],
            'citypass': ['citypass.com', 'citypass'],
            'gocity': ['gocity.com', 'smartdestinations'],
            'isango': ['isango.com', 'isango'],
            'attractiontix': ['attractiontix.com'],
            'veltra': ['veltra.com', 'veltra']
        }
        
        # Progress tracking
        self.stats = {
            'total_companies': 0,
            'completed': 0,
            'failed': 0,
            'high_value_prospects': 0,
            'good_prospects': 0,
            'medium_prospects': 0,
            'non_prospects': 0,
            'start_time': None,
            'phase': 'Aggressive Processing'
        }

    def print_dashboard(self):
        """Print real-time processing dashboard."""
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            elapsed_str = f"{int(elapsed//3600)}h {int((elapsed%3600)//60)}m"
            
            if self.stats['completed'] > 0:
                rate = self.stats['completed'] / (elapsed / 3600)  # companies per hour
                remaining = self.stats['total_companies'] - self.stats['completed'] - self.stats['failed']
                eta = remaining / rate if rate > 0 else 0
                eta_str = f"{int(eta//1)}h {int((eta%1)*60)}m"
            else:
                rate = 0
                eta_str = "Calculating..."
        else:
            elapsed_str = "0h 0m"
            eta_str = "Calculating..."
            rate = 0
        
        completed_total = self.stats['completed'] + self.stats['failed']
        progress_pct = (completed_total / self.stats['total_companies'] * 100) if self.stats['total_companies'] > 0 else 0
        success_rate = (self.stats['completed'] / completed_total * 100) if completed_total > 0 else 0
        
        print("\n" + "="*80)
        print("🎯 CONTINUOUS PROCESSING DASHBOARD")
        print("="*80)
        print(f"📊 OVERALL PROGRESS: {completed_total:,} / {self.stats['total_companies']:,} companies ({progress_pct:.1f}% complete)")
        print(f"⚡ CURRENT PHASE: {self.stats['phase']}")
        print(f"⏱️  ESTIMATED TIME REMAINING: {eta_str}")
        print(f"📈 PROCESSING RATE: {rate:.1f} companies/hour")
        
        print(f"\n📈 SUCCESS RATES:")
        print(f"✅ Successfully analyzed: {self.stats['completed']:,} companies ({success_rate:.1f}%)")
        print(f"❌ Failed (will retry): {self.stats['failed']:,} companies")
        
        print(f"\n🎯 PROSPECT DISCOVERY:")
        print(f"🔥 High-value prospects: {self.stats['high_value_prospects']}")
        print(f"✅ Good prospects: {self.stats['good_prospects']}")
        print(f"⚠️  Medium prospects: {self.stats['medium_prospects']}")
        print(f"❌ Non-prospects (have chatbots): {self.stats['non_prospects']}")
        
        print(f"\n⏰ RUNTIME: {elapsed_str}")
        print(f"🖥️  CURRENT SETTINGS: {self.current_settings['concurrency']} parallel | {self.current_settings['timeout']/1000}s timeout")
        print("="*80)

    def analyze_page_content(self, html_content, page_text, page_url):
        """Analyze page content for chatbots, booking tech, and OTA dependencies."""
        html_lower = html_content.lower()
        text_lower = page_text.lower()
        combined_content = html_lower + " " + text_lower
        
        results = {
            'has_chatbot': False,
            'chatbot_types': [],
            'booking_technology': [],
            'ota_dependencies': [],
            'analysis_details': {}
        }
        
        # 1. KNOWN PATTERN DETECTION
        for chatbot_type, patterns in self.chatbot_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    results['has_chatbot'] = True
                    if chatbot_type not in results['chatbot_types']:
                        results['chatbot_types'].append(chatbot_type)
                    break
        
        # 2. BEHAVIORAL CHATBOT DETECTION
        if not results['has_chatbot']:
            chatbot_indicators = self._detect_unknown_chatbot(html_content, page_text)
            if chatbot_indicators['likely_chatbot']:
                results['has_chatbot'] = True
                results['chatbot_types'].extend(chatbot_indicators['evidence'])
        
        # 3. KNOWN BOOKING TECHNOLOGY DETECTION
        for booking_type, patterns in self.booking_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    if booking_type not in results['booking_technology']:
                        results['booking_technology'].append(booking_type)
                    break
        
        # 4. BEHAVIORAL BOOKING DETECTION
        unknown_booking = self._detect_unknown_booking_system(html_content, page_text)
        results['booking_technology'].extend(unknown_booking)
        
        # 5. KNOWN OTA DETECTION
        for ota_type, patterns in self.ota_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    if ota_type not in results['ota_dependencies']:
                        results['ota_dependencies'].append(ota_type)
                    break
        
        # 6. BEHAVIORAL OTA DETECTION
        unknown_ota = self._detect_unknown_ota_integration(html_content, page_text, page_url)
        results['ota_dependencies'].extend(unknown_ota)
        
        # Enhanced analysis
        results['analysis_details'] = {
            'has_online_booking': any(keyword in combined_content for keyword in 
                                    ['book online', 'book now', 'reserve now', 'buy tickets', 'purchase']),
            'has_contact_form': any(keyword in combined_content for keyword in 
                                  ['contact form', 'contact us', 'get in touch', 'enquiry']),
            'mentions_commission': any(keyword in combined_content for keyword in 
                                     ['commission', 'booking fee', 'service fee']),
            'has_live_chat_ui': self._has_chat_ui_elements(html_content),
            'has_booking_widgets': self._has_booking_widgets(html_content),
            'external_booking_links': self._count_external_booking_links(html_content, page_url)
        }
        
        return results

    def _detect_unknown_chatbot(self, html_content, page_text):
        """Detect chatbots using behavioral analysis."""
        evidence = []
        score = 0
        
        chat_ui_indicators = [
            r'class="[^"]*chat[^"]*"', r'class="[^"]*message[^"]*"',
            r'class="[^"]*widget[^"]*"', r'class="[^"]*bubble[^"]*"',
            r'id="[^"]*chat[^"]*"', r'id="[^"]*message[^"]*"',
            r'<div[^>]*chat[^>]*>', r'<iframe[^>]*chat[^>]*>',
            r'data-[^=]*chat[^=]*=', r'aria-label="[^"]*chat[^"]*"',
            r'function[^{]*chat[^{]*{', r'\.chat\s*\(',
            r'chat\s*:', r'chatbot', r'livechat'
        ]
        
        for pattern in chat_ui_indicators:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                score += len(matches)
                evidence.append(f"chat_ui_elements ({len(matches)} found)")
        
        chat_text_indicators = [
            'start chat', 'chat with us', 'live chat', 'chat now',
            'send message', 'type your message', 'chat support',
            'online support', 'ask us anything', 'need help?',
            'how can we help', 'chat bubble', 'minimize chat'
        ]
        
        for indicator in chat_text_indicators:
            if indicator in page_text.lower():
                score += 2
                evidence.append(f"chat_text: '{indicator}'")
        
        return {
            'likely_chatbot': score >= 5,
            'confidence_score': score,
            'evidence': evidence if score >= 5 else []
        }

    def _detect_unknown_booking_system(self, html_content, page_text):
        """Detect booking systems using behavioral analysis."""
        booking_systems = []
        
        booking_form_patterns = [
            r'<form[^>]*book[^>]*>', r'<form[^>]*reserv[^>]*>',
            r'<form[^>]*ticket[^>]*>', r'input[^>]*date[^>]*',
            r'select[^>]*guest[^>]*>', r'select[^>]*person[^>]*>',
            r'input[^>]*quantity[^>]*>', r'button[^>]*book[^>]*>'
        ]
        
        form_matches = sum(1 for pattern in booking_form_patterns 
                          if re.search(pattern, html_content, re.IGNORECASE))
        
        if form_matches >= 3:
            booking_systems.append('custom_booking_form')
        
        calendar_patterns = [
            'datepicker', 'calendar-widget', 'date-selector',
            'flatpickr', 'pikaday', 'datejs', 'moment.js'
        ]
        
        if any(pattern in html_content.lower() for pattern in calendar_patterns):
            booking_systems.append('calendar_booking_widget')
        
        payment_patterns = [
            'payment-form', 'credit-card', 'card-number',
            'billing-address', 'cvv', 'expiry'
        ]
        
        if any(pattern in html_content.lower() for pattern in payment_patterns):
            booking_systems.append('integrated_payment_system')
        
        return booking_systems

    def _detect_unknown_ota_integration(self, html_content, page_text, page_url):
        """Detect OTA dependencies using behavioral analysis."""
        ota_integrations = []
        
        try:
            domain = urlparse(page_url).netloc
            
            external_booking_patterns = [
                r'href="https?://(?!' + re.escape(domain) + r').*book',
                r'href="https?://(?!' + re.escape(domain) + r').*reserv',
                r'href="https?://(?!' + re.escape(domain) + r').*ticket',
            ]
            
            external_links = []
            for pattern in external_booking_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                external_links.extend(matches)
            
            if len(external_links) >= 2:
                ota_integrations.append('external_booking_redirects')
        except Exception:
            pass
        
        return ota_integrations

    def _has_chat_ui_elements(self, html_content):
        """Check for chat UI elements in the HTML."""
        chat_ui_selectors = [
            'chat-widget', 'chat-bubble', 'chat-button',
            'message-input', 'chat-container', 'live-chat'
        ]
        return any(selector in html_content.lower() for selector in chat_ui_selectors)

    def _has_booking_widgets(self, html_content):
        """Check for booking widget elements."""
        booking_selectors = [
            'booking-widget', 'reservation-form', 'book-now',
            'date-picker', 'guest-selector', 'booking-calendar'
        ]
        return any(selector in html_content.lower() for selector in booking_selectors)

    def _count_external_booking_links(self, html_content, page_url):
        """Count links that redirect to external booking platforms."""
        try:
            domain = urlparse(page_url).netloc
            link_pattern = r'href="(https?://(?!' + re.escape(domain) + r')[^"]*(?:book|reserv|ticket|buy)[^"]*)"'
            external_booking_links = re.findall(link_pattern, html_content, re.IGNORECASE)
            return len(external_booking_links)
        except Exception:
            return 0

    def _generate_prospect_evaluation(self, analysis):
        """Generate prospect evaluation based on analysis."""
        has_chatbot = analysis.get('has_chatbot', False)
        booking_tech = analysis.get('booking_technology', [])
        ota_deps = analysis.get('ota_dependencies', [])
        details = analysis.get('analysis_details', {})
        
        if not has_chatbot:
            if booking_tech or details.get('has_online_booking'):
                if ota_deps:
                    return "HIGH-VALUE PROSPECT"
                else:
                    return "GOOD PROSPECT"
            else:
                return "MEDIUM PROSPECT"
        else:
            return "NOT A PROSPECT"

    def _generate_chatbot_summary(self, analysis):
        """Generate chatbot analysis summary."""
        has_chatbot = analysis.get('has_chatbot', False)
        chatbot_types = analysis.get('chatbot_types', [])
        
        if has_chatbot:
            if chatbot_types:
                return f"YES - {', '.join(chatbot_types[:3])}"
            else:
                return "YES - Unknown type"
        else:
            return "NO"

    def _generate_booking_summary(self, analysis):
        """Generate booking technology summary."""
        booking_tech = analysis.get('booking_technology', [])
        
        if booking_tech:
            known_platforms = [tech for tech in booking_tech if not tech.startswith(('custom_', 'calendar_', 'integrated_', 'dynamic_', 'network_'))]
            if known_platforms:
                return ', '.join(known_platforms[:3])
            else:
                return f"Custom system"
        else:
            return "None detected"

    def _generate_ota_summary(self, analysis):
        """Generate OTA dependencies summary."""
        ota_deps = analysis.get('ota_dependencies', [])
        
        if ota_deps:
            known_otas = [ota for ota in ota_deps if not ota.startswith(('external_', 'ota_style_', 'availability_'))]
            if known_otas:
                return ', '.join(known_otas[:2])
            else:
                return "External dependencies detected"
        else:
            return "None detected"

    async def analyze_page(self, page, url, timeout):
        """Analyze a single page."""
        try:
            await page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)
            
            html_content = await page.content()
            text_content = await page.evaluate('document.body.innerText || ""')
            
            analysis = self.analyze_page_content(html_content, text_content, url)
            
            # Enhanced dynamic detection
            await self._detect_dynamic_elements(page, analysis)
            
            # Network analysis
            network_analysis = await self._analyze_network_requests(page)
            analysis['chatbot_types'].extend(network_analysis['chatbot_services'])
            analysis['booking_technology'].extend(network_analysis['booking_services'])
            analysis['ota_dependencies'].extend(network_analysis['ota_services'])
            
            if network_analysis['chatbot_services']:
                analysis['has_chatbot'] = True
            
            return analysis
            
        except Exception as e:
            logger.warning(f"Error analyzing {url}: {e}")
            return {
                'has_chatbot': None,
                'chatbot_types': [],
                'booking_technology': [],
                'ota_dependencies': [],
                'analysis_details': {},
                'error': str(e)
            }

    async def _detect_dynamic_elements(self, page, analysis):
        """Detect dynamic elements."""
        try:
            await page.wait_for_timeout(2000)
            
            chatbot_selectors = [
                '[class*="chat"]:not([class*="chart"])', '[id*="chat"]:not([id*="chart"])',
                '[class*="widget"]', '[id*="widget"]',
                '[class*="messenger"]', '[id*="messenger"]'
            ]
            
            for selector in chatbot_selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    for element in elements[:3]:
                        try:
                            element_text = await element.inner_text()
                            is_visible = await element.is_visible()
                            
                            chat_keywords = [
                                'start chat', 'chat with', 'live chat', 'support chat',
                                'help chat', 'message us', 'ask question', 'need help'
                            ]
                            
                            has_chat_keywords = any(keyword in element_text.lower() for keyword in chat_keywords)
                            
                            if has_chat_keywords and is_visible:
                                analysis['has_chatbot'] = True
                                analysis['chatbot_types'].append('dynamic_chat_widget')
                                break
                        except:
                            continue
                    
        except Exception:
            pass

    async def _analyze_network_requests(self, page):
        """Analyze network requests."""
        network_analysis = {
            'chatbot_services': [],
            'booking_services': [],
            'ota_services': []
        }
        
        try:
            network_requests = await page.evaluate('''
                () => {
                    const requests = [];
                    if (window.performance && window.performance.getEntriesByType) {
                        const entries = window.performance.getEntriesByType('resource');
                        for (const entry of entries) {
                            if (entry.name.includes('http')) {
                                requests.push(entry.name);
                            }
                        }
                    }
                    return requests;
                }
            ''')
            
            for request_url in network_requests:
                url_lower = request_url.lower()
                
                chatbot_domains = ['chat', 'support', 'widget', 'messenger', 'livechat', 'zendesk', 'intercom', 'drift', 'crisp', 'tidio', 'tawk']
                
                if any(domain in url_lower for domain in chatbot_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['chatbot_services']]:
                        network_analysis['chatbot_services'].append(f'network_{domain}')
                
                booking_domains = ['booking', 'reserv', 'ticket', 'payment', 'checkout', 'stripe', 'paypal', 'square']
                
                if any(domain in url_lower for domain in booking_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['booking_services']]:
                        network_analysis['booking_services'].append(f'network_{domain}')
            
        except Exception:
            pass
        
        return network_analysis

    async def analyze_website(self, browser, url, timeout):
        """Analyze a website across multiple pages."""
        all_results = {
            'has_chatbot': False,
            'chatbot_types': set(),
            'booking_technology': set(),
            'ota_dependencies': set(),
            'analysis_details': {},
            'pages_analyzed': 0
        }
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        )
        page = await context.new_page()
        
        try:
            # Analyze main page
            main_analysis = await self.analyze_page(page, url, timeout)
            if 'error' not in main_analysis:
                all_results['has_chatbot'] = main_analysis['has_chatbot']
                all_results['chatbot_types'].update(main_analysis['chatbot_types'])
                all_results['booking_technology'].update(main_analysis['booking_technology'])
                all_results['ota_dependencies'].update(main_analysis['ota_dependencies'])
                all_results['analysis_details'] = main_analysis['analysis_details']
                all_results['pages_analyzed'] += 1
            
            # Analyze additional pages if settings allow
            if self.current_settings['max_pages_per_site'] > 1:
                try:
                    nav_links = await page.query_selector_all('nav a, .navigation a, #menu a, .menu a')
                    important_pages = ['/booking', '/book', '/tours', '/experiences']
                    
                    pages_to_check = []
                    for link in nav_links[:5]:
                        href = await link.get_attribute('href')
                        if href:
                            full_url = urljoin(url, href.strip()).rstrip('/')
                            if full_url.startswith('http') and urlparse(full_url).netloc == urlparse(url).netloc:
                                pages_to_check.append(full_url)
                    
                    for important_page in important_pages:
                        potential_url = urljoin(url, important_page).rstrip('/')
                        pages_to_check.append(potential_url)
                    
                    # Analyze up to max_pages_per_site additional pages
                    pages_checked = 1
                    for page_url in set(pages_to_check):
                        if pages_checked >= self.current_settings['max_pages_per_site']:
                            break
                        
                        try:
                            page_analysis = await self.analyze_page(page, page_url, timeout)
                            if 'error' not in page_analysis:
                                if page_analysis['has_chatbot']:
                                    all_results['has_chatbot'] = True
                                all_results['chatbot_types'].update(page_analysis['chatbot_types'])
                                all_results['booking_technology'].update(page_analysis['booking_technology'])
                                all_results['ota_dependencies'].update(page_analysis['ota_dependencies'])
                                all_results['pages_analyzed'] += 1
                            
                            pages_checked += 1
                            await asyncio.sleep(1)
                            
                        except Exception:
                            continue
                    
                except Exception:
                    pass
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            all_results['error'] = str(e)
        finally:
            await context.close()
        
        # Convert sets to lists
        return {
            'has_chatbot': all_results['has_chatbot'],
            'chatbot_types': list(all_results['chatbot_types']),
            'booking_technology': list(all_results['booking_technology']),
            'ota_dependencies': list(all_results['ota_dependencies']),
            'analysis_details': all_results['analysis_details'],
            'pages_analyzed': all_results['pages_analyzed']
        }

    def clean_url(self, url):
        """Clean and validate a URL."""
        if not url or pd.isna(url):
            return None
        url = str(url).strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

    def backup_progress(self, df, backup_name):
        """Create backup of current progress."""
        backup_file = f"backup_{backup_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_file, index=False)
        logger.info(f"Created backup: {backup_file}")

    def update_statistics(self, df):
        """Update processing statistics."""
        # Count prospect types
        self.stats['high_value_prospects'] = len(df[df['prospect_evaluation'] == 'HIGH-VALUE PROSPECT'])
        self.stats['good_prospects'] = len(df[df['prospect_evaluation'] == 'GOOD PROSPECT'])
        self.stats['medium_prospects'] = len(df[df['prospect_evaluation'] == 'MEDIUM PROSPECT'])
        self.stats['non_prospects'] = len(df[df['prospect_evaluation'] == 'NOT A PROSPECT'])
        
        # Count completed and failed
        completed_mask = (~df['has_chatbot'].isna()) & (~df['has_chatbot'].str.contains('Error', na=False))
        self.stats['completed'] = len(df[completed_mask])
        self.stats['failed'] = len(df[df['has_chatbot'].str.contains('Error', na=False)])

    async def process_batch(self, df, batch_indices, output_csv):
        """Process a single batch of companies."""
        batch_data = df.loc[batch_indices].copy()
        
        # Find URL column
        url_columns = ['Website URL', 'website url', 'url', 'domain', 'website', 'company_url', 'site', 'Website', 'URL', 'Domain']
        url_column = next((col for col in url_columns if col in df.columns), None)
        
        if not url_column:
            logger.error("No URL column found in CSV!")
            return
        
        # Run parallel analysis
        semaphore = asyncio.Semaphore(self.current_settings['concurrency'])
        
        async def analyze_with_semaphore(browser, index, company_name, url):
            async with semaphore:
                try:
                    analysis = await self.analyze_website(browser, url, self.current_settings['timeout'])
                    
                    # Update dataframe with results (ONLY update this specific row)
                    df.loc[index, 'has_chatbot'] = 'Yes' if analysis['has_chatbot'] else 'No'
                    df.loc[index, 'chatbot_analysis'] = self._generate_chatbot_summary(analysis)
                    df.loc[index, 'chatbot_types_detailed'] = '; '.join(analysis['chatbot_types']) if analysis['chatbot_types'] else 'None detected'
                    df.loc[index, 'booking_technology_summary'] = self._generate_booking_summary(analysis)
                    df.loc[index, 'booking_technology_detailed'] = '; '.join(analysis['booking_technology']) if analysis['booking_technology'] else 'None detected'
                    df.loc[index, 'ota_analysis'] = self._generate_ota_summary(analysis)
                    df.loc[index, 'ota_dependencies_detailed'] = '; '.join(analysis['ota_dependencies']) if analysis['ota_dependencies'] else 'None detected'
                    df.loc[index, 'prospect_evaluation'] = self._generate_prospect_evaluation(analysis)
                    df.loc[index, 'pages_analyzed'] = analysis.get('pages_analyzed', 0)
                    df.loc[index, 'has_contact_form'] = 'Yes' if analysis.get('analysis_details', {}).get('has_contact_form') else 'No'
                    df.loc[index, 'has_online_booking'] = 'Yes' if analysis.get('analysis_details', {}).get('has_online_booking') else 'No'
                    df.loc[index, 'external_booking_links'] = analysis.get('analysis_details', {}).get('external_booking_links', 0)
                    df.loc[index, 'analysis_confidence'] = "High" if analysis.get('pages_analyzed', 0) >= 3 else "Medium" if analysis.get('pages_analyzed', 0) >= 2 else "Low"
                    df.loc[index, 'last_analyzed'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df.loc[index, 'analysis_status'] = 'COMPLETED'
                    
                    logger.info(f"✅ COMPLETED: {company_name} | {df.loc[index, 'prospect_evaluation']}")
                    
                except Exception as e:
                    error_msg = f"Error: {str(e)[:100]}"
                    df.loc[index, 'has_chatbot'] = error_msg
                    df.loc[index, 'analysis_status'] = 'FAILED'
                    df.loc[index, 'last_analyzed'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    logger.error(f"❌ FAILED: {company_name} - {error_msg}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            tasks = []
            
            for index in batch_indices:
                row = df.loc[index]
                clean_url = self.clean_url(row.get(url_column))
                if clean_url:
                    company_name = row.get('Company Name', f'Company at index {index}')
                    tasks.append(analyze_with_semaphore(browser, index, company_name, clean_url))
                else:
                    df.loc[index, 'has_chatbot'] = 'Invalid URL'
                    df.loc[index, 'analysis_status'] = 'INVALID_URL'
            
            if tasks:
                await asyncio.gather(*tasks)
            await browser.close()
        
        # Save progress after each batch
        df.to_csv(output_csv, index=False)
        self.update_statistics(df)

    async def continuous_process(self, input_csv, output_csv):
        """Main continuous processing loop."""
        logger.info("🚀 Starting continuous processing...")
        
        # Load data
        df = pd.read_csv(input_csv)
        self.stats['total_companies'] = len(df)
        self.stats['start_time'] = time.time()
        
        # Initialize new columns if they don't exist (PRESERVE existing data)
        analysis_columns = [
            'has_chatbot', 'chatbot_analysis', 'chatbot_types_detailed',
            'booking_technology_summary', 'booking_technology_detailed', 
            'ota_analysis', 'ota_dependencies_detailed',
            'prospect_evaluation', 'pages_analyzed',
            'has_contact_form', 'has_online_booking', 'external_booking_links',
            'analysis_confidence', 'last_analyzed', 'analysis_status'
        ]
        
        for col in analysis_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Create initial backup
        self.backup_progress(df, "initial")
        
        # Phase 1: Aggressive Processing
        logger.info("🔥 Phase 1: Aggressive Processing")
        self.stats['phase'] = 'Aggressive Processing'
        self.current_settings = self.aggressive_settings.copy()
        
        unprocessed = df[df['analysis_status'].isna()].index.tolist()
        
        while unprocessed:
            batch_size = self.current_settings['batch_size']
            batch_indices = unprocessed[:batch_size]
            
            logger.info(f"Processing batch: {len(batch_indices)} companies")
            self.print_dashboard()
            
            await self.process_batch(df, batch_indices, output_csv)
            
            # Remove processed companies from unprocessed list
            unprocessed = df[df['analysis_status'].isna()].index.tolist()
            
            # Break between batches
            if unprocessed:
                logger.info(f"Batch complete. Resting for {self.current_settings['delay_between_batches']} seconds...")
                await asyncio.sleep(self.current_settings['delay_between_batches'])
            
            # Create backup every 5 batches
            if len(df[df['analysis_status'] == 'COMPLETED']) % (batch_size * 5) == 0:
                self.backup_progress(df, "phase1_progress")
        
        # Phase 2: Conservative Retry for Failed Companies
        failed_companies = df[df['analysis_status'] == 'FAILED'].index.tolist()
        
        if failed_companies:
            logger.info("🔄 Phase 2: Conservative Retry for Failed Companies")
            self.stats['phase'] = 'Conservative Retry'
            self.current_settings = self.conservative_settings.copy()
            
            # Reset failed companies for retry
            df.loc[failed_companies, 'analysis_status'] = pd.NA
            
            unprocessed = failed_companies
            
            while unprocessed:
                batch_size = self.current_settings['batch_size']
                batch_indices = unprocessed[:batch_size]
                
                logger.info(f"Retrying batch: {len(batch_indices)} companies")
                self.print_dashboard()
                
                await self.process_batch(df, batch_indices, output_csv)
                
                # Remove processed companies from unprocessed list
                unprocessed = df[(df.index.isin(failed_companies)) & (df['analysis_status'].isna())].index.tolist()
                
                # Break between batches
                if unprocessed:
                    await asyncio.sleep(self.current_settings['delay_between_batches'])
        
        # Phase 3: Patient Processing for Still-Failed Companies
        still_failed = df[df['analysis_status'] == 'FAILED'].index.tolist()
        
        if still_failed:
            logger.info("🐌 Phase 3: Patient Processing for Stubborn Sites")
            self.stats['phase'] = 'Patient Processing'
            self.current_settings = self.patient_settings.copy()
            
            # Reset for final retry
            df.loc[still_failed, 'analysis_status'] = pd.NA
            
            unprocessed = still_failed
            
            while unprocessed:
                batch_size = self.current_settings['batch_size']
                batch_indices = unprocessed[:batch_size]
                
                logger.info(f"Final attempt batch: {len(batch_indices)} companies")
                self.print_dashboard()
                
                await self.process_batch(df, batch_indices, output_csv)
                
                # Remove processed companies from unprocessed list  
                unprocessed = df[(df.index.isin(still_failed)) & (df['analysis_status'].isna())].index.tolist()
                
                if unprocessed:
                    await asyncio.sleep(self.current_settings['delay_between_batches'])
        
        # Final results
        self.print_final_report(df, output_csv)

    def print_final_report(self, df, output_csv):
        """Print comprehensive final analysis report."""
        self.update_statistics(df)
        
        # Calculate final stats
        total_completed = len(df[df['analysis_status'] == 'COMPLETED'])
        total_failed = len(df[df['analysis_status'] == 'FAILED'])
        total_invalid = len(df[df['analysis_status'] == 'INVALID_URL'])
        
        success_rate = (total_completed / len(df) * 100) if len(df) > 0 else 0
        
        elapsed = time.time() - self.stats['start_time']
        elapsed_str = f"{int(elapsed//3600)}h {int((elapsed%3600)//60)}m"
        
        print("\n" + "="*100)
        print("🎯 FINAL ANALYSIS REPORT")
        print("="*100)
        print(f"📊 TOTAL COMPANIES PROCESSED: {len(df):,}")
        print(f"✅ Successfully analyzed: {total_completed:,} ({success_rate:.1f}%)")
        print(f"❌ Failed to analyze: {total_failed:,}")
        print(f"🚫 Invalid URLs: {total_invalid:,}")
        print(f"⏰ Total processing time: {elapsed_str}")
        
        print(f"\n🎯 PROSPECT BREAKDOWN:")
        print(f"🔥 HIGH-VALUE PROSPECTS: {self.stats['high_value_prospects']:,} (No chatbot + Booking + OTA dependent)")
        print(f"✅ GOOD PROSPECTS: {self.stats['good_prospects']:,} (No chatbot + Booking capability)")
        print(f"⚠️  MEDIUM PROSPECTS: {self.stats['medium_prospects']:,} (No chatbot + Limited booking)")
        print(f"❌ NON-PROSPECTS: {self.stats['non_prospects']:,} (Already have chatbots)")
        
        total_prospects = self.stats['high_value_prospects'] + self.stats['good_prospects'] + self.stats['medium_prospects']
        prospect_rate = (total_prospects / total_completed * 100) if total_completed > 0 else 0
        
        print(f"\n📈 KEY METRICS:")
        print(f"🎯 Total qualified prospects: {total_prospects:,}")
        print(f"📊 Prospect conversion rate: {prospect_rate:.1f}%")
        print(f"💎 High-value prospect rate: {(self.stats['high_value_prospects']/total_completed*100):.1f}%")
        
        # Chatbot analysis
        companies_with_chatbots = len(df[df['has_chatbot'] == 'Yes'])
        companies_without_chatbots = len(df[df['has_chatbot'] == 'No'])
        
        print(f"\n💬 CHATBOT ANALYSIS:")
        print(f"✅ Companies WITH chatbots: {companies_with_chatbots:,}")
        print(f"🎯 Companies WITHOUT chatbots: {companies_without_chatbots:,}")
        
        # Top booking technologies found
        booking_tech_counts = {}
        for _, row in df.iterrows():
            if pd.notna(row.get('booking_technology_detailed')):
                techs = str(row['booking_technology_detailed']).split('; ')
                for tech in techs:
                    if tech and tech != 'None detected':
                        booking_tech_counts[tech] = booking_tech_counts.get(tech, 0) + 1
        
        if booking_tech_counts:
            print(f"\n💳 TOP BOOKING TECHNOLOGIES FOUND:")
            for tech, count in sorted(booking_tech_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"   {tech}: {count} companies")
        
        print(f"\n📁 RESULTS SAVED TO: {output_csv}")
        print(f"📋 READY FOR AIRTABLE IMPORT!")
        print("="*100)
        
        # Create a summary report file
        summary_file = f"analysis_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(summary_file, 'w') as f:
            f.write(f"Analysis Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Companies: {len(df)}\n")
            f.write(f"Successfully Analyzed: {total_completed}\n")
            f.write(f"High-Value Prospects: {self.stats['high_value_prospects']}\n")
            f.write(f"Good Prospects: {self.stats['good_prospects']}\n")
            f.write(f"Medium Prospects: {self.stats['medium_prospects']}\n")
            f.write(f"Non-Prospects: {self.stats['non_prospects']}\n")
            f.write(f"Processing Time: {elapsed_str}\n")
        
        logger.info(f"Summary report saved to: {summary_file}")

def main():
    """Main function with simple command-line interface."""
    print("🎯 CONTINUOUS Tour Operator Website Analyzer")
    print("=" * 60)
    
    # Get inputs
    input_csv = input("Enter input CSV filename (default: companies.csv): ").strip() or 'companies.csv'
    
    if not os.path.exists(input_csv):
        print(f"❌ Error: {input_csv} not found!")
        return
    
    output_csv = input("Enter output CSV filename (default: analysis_results.csv): ").strip() or 'analysis_results.csv'
    
    # RAM detection
    try:
        import psutil
        ram_gb = round(psutil.virtual_memory().total / (1024**3))
        print(f"🖥️  Detected {ram_gb}GB RAM")
    except:
        ram_gb = int(input("Enter your RAM in GB (default: 16): ").strip() or '16')
    
    # Confirmation
    df = pd.read_csv(input_csv)
    print(f"\n📊 Found {len(df)} companies to analyze")
    print(f"💾 Results will be saved to: {output_csv}")
    print(f"🖥️  System optimized for {ram_gb}GB RAM")
    
    confirm = input("\n🚀 Start continuous processing? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ Analysis cancelled.")
        return
    
    # Start processing
    analyzer = ContinuousTourOperatorAnalyzer(ram_gb=ram_gb)
    
    try:
        asyncio.run(analyzer.continuous_process(input_csv, output_csv))
        print("\n🎉 Analysis completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user.")
        print(f"💾 Partial results saved to: {output_csv}")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()