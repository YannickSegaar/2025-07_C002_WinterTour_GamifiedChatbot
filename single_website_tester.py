import asyncio
import re
import sys
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import logging
import json

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Reduce noise for testing
logger = logging.getLogger(__name__)

class SingleWebsiteTester:
    def __init__(self):
        # Same detection patterns as the main script
        self.timeout = 60000
        self.max_pages_per_site = 5
        
        # Detection patterns (same as main script)
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
        
        # Look for chat-related UI elements
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
        
        # Look for chat-specific text content
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
        
        # Look for chat-related JavaScript events
        js_chat_indicators = [
            'onclick.*chat', 'onload.*chat', 'chat.*function',
            'websocket', 'socket.io', 'chat.*api'
        ]
        
        for pattern in js_chat_indicators:
            if re.search(pattern, html_content, re.IGNORECASE):
                score += 3
                evidence.append(f"js_chat_function")
        
        # Check for floating action buttons
        fab_patterns = [
            r'position:\s*fixed[^}]*bottom[^}]*right',
            r'position:\s*fixed[^}]*right[^}]*bottom',
            r'class="[^"]*float[^"]*"[^>]*chat',
            r'style="[^"]*z-index:\s*999[^"]*"'
        ]
        
        for pattern in fab_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                score += 2
                evidence.append("floating_chat_button")
        
        return {
            'likely_chatbot': score >= 5,
            'confidence_score': score,
            'evidence': evidence if score >= 5 else []
        }

    def _detect_unknown_booking_system(self, html_content, page_text):
        """Detect booking systems using behavioral analysis."""
        booking_systems = []
        
        # Look for booking form structures
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
        
        # Look for calendar/date picker widgets
        calendar_patterns = [
            'datepicker', 'calendar-widget', 'date-selector',
            'flatpickr', 'pikaday', 'datejs', 'moment.js'
        ]
        
        if any(pattern in html_content.lower() for pattern in calendar_patterns):
            booking_systems.append('calendar_booking_widget')
        
        # Look for payment integration
        payment_patterns = [
            'payment-form', 'credit-card', 'card-number',
            'billing-address', 'cvv', 'expiry'
        ]
        
        if any(pattern in html_content.lower() for pattern in payment_patterns):
            booking_systems.append('integrated_payment_system')
        
        # Check for booking-related JavaScript APIs
        api_patterns = [
            r'api[/.]book', r'booking[/.]api', r'/reservation',
            r'ajax.*book', r'xhr.*reserv'
        ]
        
        if any(re.search(pattern, html_content, re.IGNORECASE) for pattern in api_patterns):
            booking_systems.append('custom_booking_api')
        
        return booking_systems

    def _detect_unknown_ota_integration(self, html_content, page_text, page_url):
        """Detect OTA dependencies using behavioral analysis."""
        ota_integrations = []
        
        try:
            domain = urlparse(page_url).netloc
            
            # Look for external booking redirects
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
        
        # Look for OTA-style pricing displays
        pricing_patterns = [
            'from.*per person', 'starting from', 'price from',
            'adult.*child.*price', 'group discount'
        ]
        
        pricing_matches = sum(1 for pattern in pricing_patterns 
                            if re.search(pattern, page_text, re.IGNORECASE))
        
        if pricing_matches >= 2:
            ota_integrations.append('ota_style_pricing')
        
        # Look for availability checking without direct booking
        availability_patterns = [
            'check availability', 'select date.*check',
            'availability calendar', 'real.*time.*availability'
        ]
        
        has_availability = any(re.search(pattern, page_text, re.IGNORECASE) 
                             for pattern in availability_patterns)
        
        has_direct_booking = any(pattern in html_content.lower() 
                               for pattern in ['<form', 'book now', 'add to cart'])
        
        if has_availability and not has_direct_booking:
            ota_integrations.append('availability_only_no_direct_booking')
        
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

    async def analyze_page(self, page, url, timeout):
        """Analyze a single page."""
        try:
            print(f"  📄 Loading page: {url}")
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
            print(f"  ❌ Error analyzing {url}: {e}")
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
                '[class*="messenger"]', '[id*="messenger"]',
                '[class*="bubble"]', '[class*="float"]',
                '[aria-label*="chat" i]', '[title*="chat" i]',
                'iframe[src*="chat"]', 'iframe[src*="widget"]',
                'button[class*="chat"]', 'button[id*="chat"]'
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
                                'help chat', 'message us', 'ask question', 'need help',
                                'contact support', 'chat now', 'talk to us'
                            ]
                            
                            has_chat_keywords = any(keyword in element_text.lower() for keyword in chat_keywords)
                            
                            if has_chat_keywords and is_visible:
                                analysis['has_chatbot'] = True
                                analysis['chatbot_types'].append('dynamic_chat_widget')
                                break
                        except:
                            continue
            
            # Check for booking elements
            booking_selectors = [
                '[class*="book"]', '[id*="book"]',
                '[class*="reserv"]', '[id*="reserv"]',
                'form[action*="book"]', 'form[action*="reserv"]'
            ]
            
            for selector in booking_selectors:
                elements = await page.query_selector_all(selector)
                if len(elements) >= 2:
                    analysis['booking_technology'].append('dynamic_booking_system')
                    break
                    
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
                
                # Check for chatbot services
                chatbot_domains = [
                    'chat', 'support', 'widget', 'messenger', 'livechat',
                    'helpdesk', 'zendesk', 'intercom', 'drift', 'crisp',
                    'tidio', 'tawk', 'olark', 'freshchat', 'purechat'
                ]
                
                if any(domain in url_lower for domain in chatbot_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['chatbot_services']]:
                        network_analysis['chatbot_services'].append(f'network_{domain}')
                
                # Check for booking services
                booking_domains = [
                    'booking', 'reserv', 'ticket', 'payment', 'checkout',
                    'stripe', 'paypal', 'square', 'calendar'
                ]
                
                if any(domain in url_lower for domain in booking_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['booking_services']]:
                        network_analysis['booking_services'].append(f'network_{domain}')
                
                # Check for OTA services
                ota_domains = [
                    'getyourguide', 'viator', 'tripadvisor', 'expedia',
                    'booking.com', 'klook', 'tiqets', 'headout'
                ]
                
                if any(domain in url_lower for domain in ota_domains):
                    domain = urlparse(request_url).netloc
                    network_analysis['ota_services'].append(f'network_{domain}')
            
        except Exception:
            pass
        
        return network_analysis

    def clean_url(self, url):
        """Clean and validate a URL."""
        if not url:
            return None
        url = str(url).strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

    def print_analysis_results(self, analysis, url):
        """Print detailed analysis results to terminal."""
        print("\n" + "="*80)
        print("🎯 WEBSITE ANALYSIS RESULTS")
        print("="*80)
        print(f"🌐 URL: {url}")
        print(f"📊 Pages Analyzed: {analysis.get('pages_analyzed', 1)}")
        
        # Chatbot Analysis
        print("\n" + "💬 CHATBOT ANALYSIS")
        print("-" * 40)
        has_chatbot = analysis.get('has_chatbot', False)
        if has_chatbot:
            print("✅ HAS CHATBOT: YES")
            chatbot_types = analysis.get('chatbot_types', [])
            if chatbot_types:
                print("📝 Chatbot Types Found:")
                for i, chatbot in enumerate(chatbot_types, 1):
                    print(f"   {i}. {chatbot}")
            else:
                print("📝 Chatbot Types: None specified")
        else:
            print("❌ HAS CHATBOT: NO")
            print("🎯 PROSPECT STATUS: ✅ QUALIFIED (No chatbot detected)")
        
        # Booking Technology Analysis
        print("\n" + "💳 BOOKING TECHNOLOGY ANALYSIS")
        print("-" * 40)
        booking_tech = analysis.get('booking_technology', [])
        if booking_tech:
            print("✅ BOOKING SYSTEMS DETECTED:")
            for i, tech in enumerate(booking_tech, 1):
                print(f"   {i}. {tech}")
        else:
            print("❌ NO BOOKING SYSTEMS DETECTED")
        
        # OTA Dependencies Analysis
        print("\n" + "🏢 OTA DEPENDENCIES ANALYSIS")
        print("-" * 40)
        ota_deps = analysis.get('ota_dependencies', [])
        if ota_deps:
            print("✅ OTA DEPENDENCIES FOUND:")
            for i, ota in enumerate(ota_deps, 1):
                print(f"   {i}. {ota}")
            print("💡 INSIGHT: Company may be paying high commissions to OTAs")
        else:
            print("❌ NO OTA DEPENDENCIES DETECTED")
        
        # Additional Details
        print("\n" + "🔍 ADDITIONAL ANALYSIS")
        print("-" * 40)
        details = analysis.get('analysis_details', {})
        print(f"📞 Has Contact Form: {'✅ Yes' if details.get('has_contact_form') else '❌ No'}")
        print(f"🛒 Has Online Booking: {'✅ Yes' if details.get('has_online_booking') else '❌ No'}")
        print(f"💰 Mentions Commission: {'✅ Yes' if details.get('mentions_commission') else '❌ No'}")
        print(f"💬 Has Chat UI Elements: {'✅ Yes' if details.get('has_live_chat_ui') else '❌ No'}")
        print(f"📅 Has Booking Widgets: {'✅ Yes' if details.get('has_booking_widgets') else '❌ No'}")
        print(f"🔗 External Booking Links: {details.get('external_booking_links', 0)}")
        
        # Final Recommendation
        print("\n" + "🎯 PROSPECT EVALUATION")
        print("-" * 40)
        if not has_chatbot:
            if booking_tech or details.get('has_online_booking'):
                if ota_deps:
                    print("🔥 HIGH-VALUE PROSPECT: No chatbot + Has booking capability + OTA dependent")
                else:
                    print("✅ GOOD PROSPECT: No chatbot + Has booking capability")
            else:
                print("⚠️  MEDIUM PROSPECT: No chatbot but limited booking capability")
        else:
            print("❌ NOT A PROSPECT: Already has chatbot solution")
        
        print("="*80)

    async def test_single_website(self, url):
        """Test analysis on a single website."""
        print(f"🚀 Starting analysis of: {url}")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
            )
            page = await context.new_page()
            
            try:
                domain = urlparse(url).netloc
                visited_urls = {url.rstrip('/')}
                
                # Analyze main page
                print("📄 Analyzing main page...")
                main_analysis = await self.analyze_page(page, url, self.timeout)
                
                if 'error' in main_analysis:
                    print(f"❌ Error: {main_analysis['error']}")
                    return
                
                all_results = {
                    'has_chatbot': main_analysis['has_chatbot'],
                    'chatbot_types': set(main_analysis['chatbot_types']),
                    'booking_technology': set(main_analysis['booking_technology']),
                    'ota_dependencies': set(main_analysis['ota_dependencies']),
                    'analysis_details': main_analysis['analysis_details'],
                    'pages_analyzed': 1
                }
                
                # Analyze additional pages
                print("📄 Looking for additional pages to analyze...")
                important_pages = ['/booking', '/book', '/tours', '/experiences', '/reserve', '/contact']
                links_to_check = []
                
                try:
                    nav_links = await page.query_selector_all('nav a, .navigation a, #menu a, .menu a')
                    for link in nav_links[:5]:  # Limit for testing
                        href = await link.get_attribute('href')
                        if href:
                            full_url = urljoin(url, href.strip()).rstrip('/')
                            if full_url.startswith('http') and urlparse(full_url).netloc == domain:
                                links_to_check.append(full_url)
                    
                    for important_page in important_pages:
                        potential_url = urljoin(url, important_page).rstrip('/')
                        links_to_check.append(potential_url)
                    
                except Exception as e:
                    print(f"⚠️  Could not get navigation links: {e}")
                
                # Analyze up to 3 additional pages for testing
                pages_checked = 1
                for link_url in set(links_to_check):
                    if pages_checked >= 4 or link_url in visited_urls:  # Limit for testing
                        continue
                    
                    try:
                        page_analysis = await self.analyze_page(page, link_url, self.timeout)
                        if 'error' not in page_analysis:
                            if page_analysis['has_chatbot']:
                                all_results['has_chatbot'] = True
                            all_results['chatbot_types'].update(page_analysis['chatbot_types'])
                            all_results['booking_technology'].update(page_analysis['booking_technology'])
                            all_results['ota_dependencies'].update(page_analysis['ota_dependencies'])
                            all_results['pages_analyzed'] += 1
                        
                        visited_urls.add(link_url)
                        pages_checked += 1
                        await asyncio.sleep(1)  # Be nice to the server
                        
                    except Exception as e:
                        print(f"⚠️  Error analyzing {link_url}: {e}")
                        continue
                
                # Convert sets to lists and print results
                final_results = {
                    'has_chatbot': all_results['has_chatbot'],
                    'chatbot_types': list(all_results['chatbot_types']),
                    'booking_technology': list(all_results['booking_technology']),
                    'ota_dependencies': list(all_results['ota_dependencies']),
                    'analysis_details': all_results['analysis_details'],
                    'pages_analyzed': all_results['pages_analyzed']
                }
                
                # Print detailed results
                self.print_analysis_results(final_results, url)
                
            except Exception as e:
                print(f"❌ Fatal error analyzing {url}: {e}")
            finally:
                await context.close()
                await browser.close()

def main():
    """Main function for single website testing."""
    print("🎯 Single Website Analysis Tester")
    print("=" * 50)
    
    # Get URL from command line argument or user input
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = input("Enter website URL to analyze: ").strip()
    
    if not url:
        print("❌ No URL provided. Exiting.")
        return
    
    # Clean and validate URL
    tester = SingleWebsiteTester()
    clean_url = tester.clean_url(url)
    
    if not clean_url:
        print("❌ Invalid URL provided. Exiting.")
        return
    
    print(f"🔍 Testing URL: {clean_url}")
    print("⏳ This may take 30-60 seconds...\n")
    
    # Run the analysis
    try:
        asyncio.run(tester.test_single_website(clean_url))
        print("\n✅ Analysis complete!")
        print("\n💡 TIP: If this is a known prospect (no chatbot), the main script should work correctly!")
        print("💡 TIP: If this shows chatbots you didn't expect, check the detailed breakdown above.")
        
    except KeyboardInterrupt:
        print("\n❌ Analysis interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error running analysis: {e}")

if __name__ == "__main__":
    main()