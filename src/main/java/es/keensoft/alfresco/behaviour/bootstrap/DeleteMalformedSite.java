package es.keensoft.alfresco.behaviour.bootstrap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.alfresco.model.ContentModel;
import org.alfresco.repo.policy.BehaviourFilter;
import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.alfresco.repo.transaction.RetryingTransactionHelper;
import org.alfresco.repo.transaction.RetryingTransactionHelper.RetryingTransactionCallback;
import org.alfresco.service.cmr.repository.NodeRef;
import org.alfresco.service.cmr.repository.NodeService;
import org.alfresco.service.cmr.repository.StoreRef;
import org.alfresco.service.cmr.search.ResultSet;
import org.alfresco.service.cmr.search.ResultSetRow;
import org.alfresco.service.cmr.search.SearchParameters;
import org.alfresco.service.cmr.search.SearchService;
import org.alfresco.service.cmr.security.AuthenticationService;
import org.alfresco.service.cmr.site.SiteInfo;
import org.alfresco.service.cmr.site.SiteService;
import org.alfresco.service.transaction.TransactionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEvent;
import org.springframework.extensions.surf.util.AbstractLifecycleBean;

public class DeleteMalformedSite extends AbstractLifecycleBean {

	private static Log logger = LogFactory.getLog(DeleteMalformedSite.class);

	private static final int DELAYED_START_SECONDS = 30;

	private NodeService nodeService;
	private SearchService searchService;
	private BehaviourFilter behaviourFilter;
	private TransactionService transactionService;
	private SiteService siteService;
	private AuthenticationService authenticationService;

	@Override
	protected void onBootstrap(ApplicationEvent event) {
		deleteMalforedSiteFolders();
	}

	private void deleteMalforedSiteFolders() {

		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
		scheduledExecutorService.schedule(new AsynchronousTask(), DELAYED_START_SECONDS, TimeUnit.SECONDS);
		scheduledExecutorService.shutdown();

	}

	private class AsynchronousTask implements Runnable {

		@Override
		public void run() {

			AuthenticationUtil.runAsSystem(new AuthenticationUtil.RunAsWork<String>() {

				@Override
				public String doWork() throws Exception {

					RetryingTransactionHelper txnHelper = transactionService.getRetryingTransactionHelper();
					txnHelper.setForceWritable(true);
					txnHelper.doInTransaction(new RetryingTransactionCallback<Void>() {
						
						public Void execute() throws Throwable {
							SearchParameters sp = new SearchParameters();
							sp.setLanguage(SearchService.LANGUAGE_CMIS_ALFRESCO);
							sp.addStore(StoreRef.STORE_REF_WORKSPACE_SPACESSTORE);
							sp.setQuery("SELECT * FROM st:site");

							ResultSet results = null;
							try {

								results = searchService.query(sp);
								logger.info("Checking malformed sites, found " + results.length() + " sites");

								for (ResultSetRow row : results) {

									boolean siteMalformed = false;

									try {
										SiteInfo siteInfo = siteService.getSite(row.getNodeRef());
										if (siteInfo != null) {
										    siteService.getMembersRole(siteInfo.getShortName(), authenticationService.getCurrentUserName());
										} else {
											siteMalformed = (siteInfo == null);
										}
									} catch (Throwable t) {
										logger.warn("Site " + row.getNodeRef() + " is malformed!", t);
										siteMalformed = true;
									}

									if (siteMalformed) {

										NodeRef currentNodeRef = null;
										NodeRef siteParent = null;

										try {

											currentNodeRef = row.getNodeRef();
											behaviourFilter.disableBehaviour(currentNodeRef, ContentModel.ASPECT_UNDELETABLE);

											logger.info("About to delete "
													+ nodeService.getProperty(currentNodeRef, ContentModel.PROP_NAME)
													+ " folder site");

											siteParent = nodeService.getPrimaryParent(currentNodeRef).getParentRef();
											behaviourFilter.disableBehaviour(siteParent, ContentModel.ASPECT_AUDITABLE);

											logger.info("Site parent folder is "
													+ nodeService.getProperty(siteParent, ContentModel.PROP_NAME)
													+ " folder");

											nodeService.deleteNode(currentNodeRef);

											logger.info("Site folder deleted");

										} catch (Throwable t) {
											logger.error("Site not deleted", t);
										} finally {
											behaviourFilter.enableBehaviour(currentNodeRef, ContentModel.ASPECT_UNDELETABLE);
											behaviourFilter.enableBehaviour(siteParent, ContentModel.ASPECT_AUDITABLE);
										}
									}
								}

								logger.info("Malformed sites checking completed");

							} finally {
								if (results != null) {
									results.close();
								}
							}

							return null;

						}

					}, false, true);

					return null;
				}

			});

		}
	}

	@Override
	protected void onShutdown(ApplicationEvent event) {
	}

	public NodeService getNodeService() {
		return nodeService;
	}

	public void setNodeService(NodeService nodeService) {
		this.nodeService = nodeService;
	}

	public SearchService getSearchService() {
		return searchService;
	}

	public void setSearchService(SearchService searchService) {
		this.searchService = searchService;
	}

	public BehaviourFilter getBehaviourFilter() {
		return behaviourFilter;
	}

	public void setBehaviourFilter(BehaviourFilter behaviourFilter) {
		this.behaviourFilter = behaviourFilter;
	}

	public TransactionService getTransactionService() {
		return transactionService;
	}

	public void setTransactionService(TransactionService transactionService) {
		this.transactionService = transactionService;
	}

	public SiteService getSiteService() {
		return siteService;
	}

	public void setSiteService(SiteService siteService) {
		this.siteService = siteService;
	}

	public AuthenticationService getAuthenticationService() {
		return authenticationService;
	}

	public void setAuthenticationService(AuthenticationService authenticationService) {
		this.authenticationService = authenticationService;
	}

}
